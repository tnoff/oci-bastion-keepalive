#!/usr/bin/env python3
"""Keep a long-lived OCI bastion port-forward to the OKE API server alive.

`generate_session.sh` opens a single bastion port-forwarding session and prints
an `ssh -L 6443:...` command you run by hand. The session has a hard TTL (max
set by the bastion), so on long debug sessions it expires, the ssh tunnel dies,
and everything pinned to 127.0.0.1:6443 -- kubectl and the MCP servers -- drops
until you regenerate it manually.

This daemon removes the manual step and the drop. It binds a small TCP relay to
127.0.0.1:6443 *once* and holds it for its whole life; your kubeconfig points at
that and is never edited again. The actual ssh tunnels live on internal ports and
rotate underneath the relay:

    kubectl / MCP -> 127.0.0.1:6443  (relay: bound once, never released)
                            |  forwards new connections to the live tunnel
              ssh A -> :7001  ...  ssh B -> :7002   (alternate each rotation)
                            |
                       bastion session -> OKE API :6443

A few minutes before the active session expires it brings up a fresh session +
ssh tunnel on the alternate internal port, health-checks it, flips the relay's
upstream to it, drains the old connections, then tears the old session down. The
front socket on 6443 is never closed, so the endpoint never disappears.

Configuration is entirely via environment variables (no OCIDs baked in):

  required:
    OKE_CLUSTER_OCID     cluster OCID (for the API server private IP)
    OKE_BASTION_OCID     bastion OCID to open sessions against

  optional:
    OCI_REGION           region (default: the region in your OCI config)
    OCI_PROFILE          OCI config profile (default: DEFAULT)
    OCI_CONFIG_FILE      OCI config path (default: ~/.oci/config)
    SESSION_TTL          requested session TTL in seconds, capped to the
                         bastion's max (default: 10800)
    ROTATE_LEAD          seconds before expiry to rotate (default: 300)
    ROTATE_RETRY         seconds to wait before re-attempting a failed rotation
                         (default: 30) -- the active tunnel keeps serving meanwhile
    DRAIN_SECONDS        grace period for old connections after flip (default: 10)
    FRONT_PORT           local port kubeconfig points at (default: 6443)
    INTERNAL_PORTS       comma-separated tunnel ports to alternate (default: 7001,7002)
    TARGET_PORT          API server port on the cluster IP (default: 6443)
    SSH_PUBLIC_KEY       public key registered with the session (default: ~/.ssh/id_rsa.pub)
    SSH_PRIVATE_KEY      private key for the tunnel (default: ~/.ssh/id_rsa)
    HEALTH_TIMEOUT       seconds to wait for a new tunnel to answer (default: 90)
    SSH_SETTLE           seconds to wait after a session goes ACTIVE before the
                         first ssh attempt, for key propagation (default: 5)
    SSH_ATTEMPTS         ssh connect attempts against one session before
                         recreating it (default: 6)
    SSH_RETRY            seconds between ssh attempts on the same session (default: 5)

Run it in the foreground (Ctrl-C cleans up sessions + tunnels):

    source venv/bin/activate          # OCI SDK + exec-plugin on PATH
    export OKE_CLUSTER_OCID=ocid1.cluster.oc1...
    export OKE_BASTION_OCID=ocid1.bastion.oc1...
    python rotate_session.py
"""

import asyncio
import http.client
import logging
import os
import shlex
import ssl
import sys
import time
from datetime import datetime, timedelta, timezone

import oci

log = logging.getLogger("session-rotator")

# Relay's only mutable state: the internal port new connections are forwarded
# to. Read live by every incoming connection, reassigned atomically on flip.
STATE = {"upstream": None}

# Hold references to fire-and-forget tasks (ssh stderr loggers) so the event
# loop doesn't garbage-collect them mid-flight.
_BG_TASKS = set()


def _spawn_bg(coro):
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


def _env(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and not val:
        sys.exit(f"error: required environment variable {name} is not set")
    return val


class Config:
    def __init__(self):
        self.cluster_ocid = _env("OKE_CLUSTER_OCID", required=True)
        self.bastion_ocid = _env("OKE_BASTION_OCID", required=True)
        self.region = _env("OCI_REGION")
        self.profile = _env("OCI_PROFILE", "DEFAULT")
        self.config_file = _env("OCI_CONFIG_FILE", oci.config.DEFAULT_LOCATION)
        self.ttl = int(_env("SESSION_TTL", "10800"))
        self.lead = int(_env("ROTATE_LEAD", "300"))
        self.rotate_retry = int(_env("ROTATE_RETRY", "30"))
        self.drain = int(_env("DRAIN_SECONDS", "10"))
        self.front_port = int(_env("FRONT_PORT", "6443"))
        self.internal_ports = [
            int(p) for p in _env("INTERNAL_PORTS", "7001,7002").split(",")
        ]
        self.target_port = int(_env("TARGET_PORT", "6443"))
        self.public_key = os.path.expanduser(
            _env("SSH_PUBLIC_KEY", "~/.ssh/id_rsa.pub")
        )
        self.private_key = os.path.expanduser(
            _env("SSH_PRIVATE_KEY", "~/.ssh/id_rsa")
        )
        self.health_timeout = int(_env("HEALTH_TIMEOUT", "90"))
        # A bastion session reports ACTIVE before its registered key propagates
        # to the SSH endpoint; settle a bit, then retry ssh on the SAME session.
        self.ssh_settle = int(_env("SSH_SETTLE", "5"))
        self.ssh_attempts = int(_env("SSH_ATTEMPTS", "6"))
        self.ssh_retry = int(_env("SSH_RETRY", "5"))


class Tunnel:
    def __init__(self, session_id, proc, port, expiry):
        self.session_id = session_id
        self.proc = proc
        self.port = port
        self.expiry = expiry


# --------------------------------------------------------------------------- #
# OCI calls (all blocking -- run via asyncio.to_thread so the relay keeps      #
# serving while a rotation is in flight).                                      #
# --------------------------------------------------------------------------- #
class Oci:
    def __init__(self, cfg):
        self.cfg = cfg
        oci_config = oci.config.from_file(cfg.config_file, cfg.profile)
        if cfg.region:
            oci_config["region"] = cfg.region
        self.bastion = oci.bastion.BastionClient(oci_config)
        self.engine = oci.container_engine.ContainerEngineClient(oci_config)

    def cluster_ip(self):
        endpoint = self.engine.get_cluster(self.cfg.cluster_ocid).data.endpoints.private_endpoint
        return endpoint.split(":")[0]

    def max_ttl(self):
        return self.bastion.get_bastion(self.cfg.bastion_ocid).data.max_session_ttl_in_seconds

    def create_session(self, cluster_ip, ttl):
        with open(self.cfg.public_key, encoding="utf-8") as fh:
            public_key = fh.read()
        target = oci.bastion.models.CreatePortForwardingSessionTargetResourceDetails(
            target_resource_private_ip_address=cluster_ip,
            target_resource_port=self.cfg.target_port,
        )
        details = oci.bastion.models.CreateSessionDetails(
            bastion_id=self.cfg.bastion_ocid,
            display_name=f"oke-rotator-{int(time.time())}",
            key_type="PUB",
            key_details=oci.bastion.models.PublicKeyDetails(public_key_content=public_key),
            session_ttl_in_seconds=ttl,
            target_resource_details=target,
        )
        return self.bastion.create_session(details).data.id

    def wait_active(self, session_id, timeout=300):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.bastion.get_session(session_id).data.lifecycle_state
            if state == "ACTIVE":
                return
            log.info("session %s is %s ...", session_id[-12:], state)
            time.sleep(5)
        raise TimeoutError(f"session {session_id} never became ACTIVE")

    def ssh_command(self, session_id):
        return self.bastion.get_session(session_id).data.ssh_metadata["command"]

    def expiry(self, session_id):
        data = self.bastion.get_session(session_id).data
        return data.time_created + timedelta(seconds=data.session_ttl_in_seconds)

    def delete_session(self, session_id):
        try:
            self.bastion.delete_session(session_id)
        except oci.exceptions.ServiceError as exc:
            log.warning("could not delete session %s: %s", session_id[-12:], exc.message)


# --------------------------------------------------------------------------- #
# ssh tunnel                                                                   #
# --------------------------------------------------------------------------- #
def build_ssh_args(metadata_command, cfg, local_port):
    """Turn the bastion's ssh-metadata command into argv for an internal port.

    The metadata looks like:
        ssh -i <privateKey> -N -L <localPort>:10.x.x.x:6443 -p 22 ocid...@host
    We substitute the real key + our internal port and add options that make ssh
    safe to run unattended (fail fast on a bad bind, die when the link goes away).
    """
    tokens = shlex.split(metadata_command)
    args = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "-i":
            args += ["-i", cfg.private_key]
            i += 2
            continue
        if tok == "-L":
            args += ["-L", tokens[i + 1].replace("<localPort>", str(local_port))]
            i += 2
            continue
        args.append(tok)
        i += 1
    # robustness options for unattended use
    args[1:1] = [
        # Offer ONLY the -i key. A bastion session accepts exactly one public
        # key; without this, ssh offers every agent identity first and can hit
        # MaxAuthTries -> "Permission denied (publickey)" before reaching ours.
        "-o", "IdentitiesOnly=yes",
        "-o", "ExitOnForwardFailure=yes",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "StrictHostKeyChecking=accept-new",
    ]
    return args


def _health_once(port):
    """One HTTPS probe through the tunnel; True if the API server answered.

    A 200 means /readyz is open; 401/403 still proves TLS + HTTP reached the real
    API server, which is all we need to know the tunnel works.
    """
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=5, context=ctx)
        conn.request("GET", "/readyz")
        status = conn.getresponse().status
        conn.close()
        return status in (200, 401, 403)
    except OSError:
        return False


async def wait_healthy(proc, port, timeout):
    """Poll the tunnel until the API answers, failing fast if ssh dies first.

    Checking proc.returncode each loop means a dead ssh (bad key, host-key
    rejection, forward failure) raises immediately instead of looping blind for
    the full timeout.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if proc.returncode is not None:
            raise RuntimeError(
                f"ssh for :{port} exited (code {proc.returncode}) before becoming healthy"
            )
        if await asyncio.to_thread(_health_once, port):
            return
        await asyncio.sleep(2)
    raise RuntimeError(f"tunnel on :{port} did not become healthy within {timeout}s")


async def _log_ssh_stderr(proc, port):
    """Surface ssh's own diagnostics (auth, host-key, forward failures) to log."""
    if proc.stderr is None:
        return
    async for line in proc.stderr:
        text = line.decode(errors="replace").rstrip()
        if text:
            log.warning("ssh[:%s] %s", port, text)


async def spawn_ssh(args, port):
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
    )
    _spawn_bg(_log_ssh_stderr(proc, port))
    return proc


async def kill_proc(proc):
    if proc.returncode is not None:
        return
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


async def open_ssh_tunnel(cfg, command, port):
    """Connect ssh for an already-ACTIVE session, retrying the SAME session.

    The bastion accepts the session's key only once it has propagated past the
    ACTIVE transition, so the first ssh attempt(s) can get a transient
    'Permission denied (publickey)'. Recreating the session re-races that window
    (the old bug); retrying ssh against the same session rides it out.
    """
    for attempt in range(1, cfg.ssh_attempts + 1):
        proc = await spawn_ssh(build_ssh_args(command, cfg, port), port)
        log.info("ssh started on :%s (pid %s), health-checking ...", port, proc.pid)
        try:
            await wait_healthy(proc, port, cfg.health_timeout)
            return proc
        except RuntimeError as exc:
            await kill_proc(proc)
            if attempt == cfg.ssh_attempts:
                raise
            log.warning("ssh to :%s not ready yet (%s); retrying same session in %ss",
                        port, exc, cfg.ssh_retry)
            await asyncio.sleep(cfg.ssh_retry)


async def bring_up(oci_client, cfg, cluster_ip, ttl, port):
    """Create a session + ssh tunnel on `port`, health-check it, return a Tunnel."""
    log.info("creating bastion session for tunnel on :%s", port)
    session_id = await asyncio.to_thread(oci_client.create_session, cluster_ip, ttl)
    await asyncio.to_thread(oci_client.wait_active, session_id)
    # Let the registered key settle before the first ssh attempt.
    await asyncio.sleep(cfg.ssh_settle)
    command = await asyncio.to_thread(oci_client.ssh_command, session_id)
    try:
        proc = await open_ssh_tunnel(cfg, command, port)
    except RuntimeError:
        await asyncio.to_thread(oci_client.delete_session, session_id)
        raise
    expiry = await asyncio.to_thread(oci_client.expiry, session_id)
    log.info("tunnel on :%s healthy; session expires %s", port, expiry.isoformat())
    return Tunnel(session_id, proc, port, expiry)


async def bring_up_with_retry(oci_client, cfg, cluster_ip, ttl, port, *, attempts=3):
    """bring_up with retries for session-level failures (transient OCI API
    errors, a session that never goes healthy). Catches broadly on purpose: a
    long-running daemon must not die on a flaky API call. CancelledError /
    KeyboardInterrupt are BaseException, so a clean shutdown still propagates."""
    for attempt in range(1, attempts + 1):
        try:
            return await bring_up(oci_client, cfg, cluster_ip, ttl, port)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if attempt == attempts:
                raise
            log.warning(
                "tunnel on :%s attempt %s/%s failed (%s); retrying in 5s",
                port, attempt, attempts, exc,
            )
            await asyncio.sleep(5)


async def retire(oci_client, tunnel):
    log.info("retiring tunnel on :%s", tunnel.port)
    await kill_proc(tunnel.proc)
    await asyncio.to_thread(oci_client.delete_session, tunnel.session_id)


# --------------------------------------------------------------------------- #
# relay                                                                        #
# --------------------------------------------------------------------------- #
async def _pipe(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except OSError:
        pass
    finally:
        if not writer.is_closing():
            writer.close()


async def handle_client(client_reader, client_writer):
    port = STATE["upstream"]  # snapshot at connect time -> in-flight conns drain cleanly
    if port is None:
        client_writer.close()
        return
    try:
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", port)
    except OSError:
        client_writer.close()
        return
    await asyncio.gather(
        _pipe(client_reader, up_writer),
        _pipe(up_reader, client_writer),
    )


# --------------------------------------------------------------------------- #
# main loop                                                                    #
# --------------------------------------------------------------------------- #
async def wait_for_rotation(active, lead):
    """Sleep until `lead` seconds before expiry, or wake early if ssh dies."""
    seconds = (active.expiry - datetime.now(timezone.utc)).total_seconds() - lead
    timer = asyncio.create_task(asyncio.sleep(max(seconds, 0)))
    death = asyncio.create_task(active.proc.wait())
    done, pending = await asyncio.wait({timer, death}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    if death in done:
        log.warning("ssh tunnel on :%s exited early -- rotating now", active.port)


async def run(cfg):
    oci_client = await asyncio.to_thread(Oci, cfg)
    cluster_ip = await asyncio.to_thread(oci_client.cluster_ip)
    max_ttl = await asyncio.to_thread(oci_client.max_ttl)
    ttl = min(cfg.ttl, max_ttl)
    if ttl < cfg.ttl:
        log.info("requested TTL %ss capped to bastion max %ss", cfg.ttl, max_ttl)
    log.info("cluster API server private IP: %s", cluster_ip)

    if len(cfg.internal_ports) < 2:
        sys.exit("error: INTERNAL_PORTS needs at least two ports to rotate")

    # Bind the front port up front so it's held for the daemon's whole life --
    # connections before the first tunnel is healthy are accepted then closed
    # (upstream is None), not refused.
    server = await asyncio.start_server(handle_client, "127.0.0.1", cfg.front_port)
    log.info("relay listening on 127.0.0.1:%s (waiting for first tunnel)", cfg.front_port)

    active = await bring_up_with_retry(oci_client, cfg, cluster_ip, ttl, cfg.internal_ports[0])
    STATE["upstream"] = active.port
    log.info("upstream -> :%s; cluster reachable on 127.0.0.1:%s", active.port, cfg.front_port)

    try:
        async with server:
            while True:
                await wait_for_rotation(active, cfg.lead)
                # Always bring the replacement up on the port the active tunnel
                # is NOT using, so we never collide with the still-bound one.
                target = next(p for p in cfg.internal_ports if p != active.port)
                try:
                    new = await bring_up_with_retry(oci_client, cfg, cluster_ip, ttl, target)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    # Rotation failed. Crucially: do NOT touch `active`. If it's
                    # still alive it keeps serving; either way we just retry.
                    if active.proc.returncode is None:
                        log.error("rotation onto :%s failed (%s); current tunnel "
                                  "still serving, retrying in %ss", target, exc, cfg.rotate_retry)
                    else:
                        log.error("rotation onto :%s failed (%s) and the active tunnel "
                                  "is DOWN -- cluster access is interrupted; retrying in %ss",
                                  target, exc, cfg.rotate_retry)
                    await asyncio.sleep(cfg.rotate_retry)
                    continue
                STATE["upstream"] = new.port
                log.info("flipped relay upstream -> :%s", new.port)
                await asyncio.sleep(cfg.drain)
                await retire(oci_client, active)
                active = new
    finally:
        log.info("shutting down -- cleaning up active session")
        await retire(oci_client, active)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    cfg = Config()
    try:
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        log.info("interrupted")


if __name__ == "__main__":
    main()
