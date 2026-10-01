# Local cluster access (OCI bastion port-forward)

How `kubectl` and the local MCP servers (see the mcp-local TechDocs) reach the
**private** OKE API server from the laptop. The [README](README.md) covers
install, usage, configuration and the managed port-forward syntax; this page is
the design and operations reference behind it.

## Why a bastion

The cluster's API endpoint is private (a `10.x.x.x:6443` VCN address), so there
is no public IP to hit. The OCI bastion service brokers a port-forward to that
address over SSH, and the laptop only ever talks to its own `127.0.0.1:6443`.
Auth to the API server is separate: the kubeconfig uses the OCI exec-plugin
(`oci ce cluster generate-token`), which runs on the host next to `kubectl`, not
inside the tunnel. The tunnel only carries TLS bytes, so rotation never affects
auth.

## One-shot path: `generate_session.sh`

`generate_session.sh` lives in the `docker-apps` checkout as a **local,
gitignored** script (it carries the cluster and bastion OCIDs, so it is never
committed; it is not on `docker-apps` main). It creates one port-forwarding
session (TTL `10800`), polls until `ACTIVE`, and prints the `ssh -L 6443:...`
command to run by hand. When the session expires the tunnel dies and everything
on `127.0.0.1:6443` drops until you re-run it.

Unlike the daemon, the script hardcodes its environment: region
`us-sanjose-1` on every `oci` call, `~/.ssh/id_rsa.pub` as the session key, and
the OCIDs inline. The daemon takes all of these from the environment instead
(`OCI_REGION`, `SSH_PUBLIC_KEY`, `SSH_PRIVATE_KEY`; defaults in
`rotate_session.env.example`). The daemon's own defaults are the region in your
OCI config and `~/.ssh/id_rsa{,.pub}`.

## Long-lived path: `rotate_session.py`

The kubeconfig is hardwired to `127.0.0.1:6443` and two ssh tunnels cannot both
bind that port, so the daemon binds a small TCP **relay** on `6443` once, for its
whole life. The ssh tunnels live on internal ports (`7001`/`7002`, alternating)
and rotate beneath it.

Rotation fires `ROTATE_LEAD` seconds (default 300) before the active session
expires, or immediately if the active ssh process exits early:

1. Create a new session through the OCI Python SDK and poll until `ACTIVE`.
2. Wait `SSH_SETTLE`, then start ssh on the alternate internal port, retrying
   against the **same** session (`SSH_ATTEMPTS` x `SSH_RETRY`); see the
   propagation gotcha below.
3. Health-check the new tunnel: HTTPS to the API returning `200`, `401` or `403`
   proves TLS and HTTP reached the real API server. The active tunnel is never
   touched until the replacement is proven healthy (`HEALTH_TIMEOUT`).
4. Flip the relay's upstream to the new port; new connections go there.
5. Sleep `DRAIN_SECONDS`, kill the old ssh and `delete_session` the old session.

If the replacement cannot be stood up (a transient OCI API error, a session that
never goes healthy) the rotation is retried after `ROTATE_RETRY` without
disturbing the active tunnel, so a failed rotation cannot take down a working
one. The requested TTL is capped to the bastion's `max_session_ttl_in_seconds`,
which is the real ceiling (an oversized `SESSION_TTL` is clamped, not an error).

The honest limit: a long-lived stream pinned to the old tunnel (`kubectl logs
-f`, a watch) still breaks when its session is torn down. The client reconnects
instantly through the stable port and lands on the new tunnel. Short
request/response calls sail through the flip.

### systemd

`rotate-session.service` runs the daemon as a `systemd --user` unit with
`Restart=on-failure` (survives logout with `loginctl enable-linger`, logs to
journald). It runs as you, so `~/.ssh`, `~/.oci` and the SSH agent work natively.
`KillSignal=SIGINT` makes `systemctl --user stop` trigger the script's cleanup,
which deletes the active session. The endpoint's continuity depends on this
process: if the daemon dies, `6443` goes with it, which is why the unit restarts
it. The unit's `WorkingDirectory`/`ExecStart` assume the checkout and venv live
at `~/Code/oci-bastion-keepalive`.

### Managed port-forwards: mechanism

Syntax and configuration are in the README. Forwarding is in-process through the
Kubernetes Python client (no `kubectl` subprocess) riding the same `127.0.0.1`
relay, so it works only once the relay is healthy. A `svc`/`deploy` target is
resolved to a Running+Ready pod on every new connection, which is why a restarted
pod or rotated session is picked up with no re-run. The exec-plugin shells out to
the `oci` CLI, so `oci-cli` is a dependency and the daemon prepends its own venv
`bin` to `PATH` (needed under `systemd`, whose `ExecStart` does not put
`venv/bin` on `PATH`).

When another service depends on a port, check it is live after any edit:
`ss -ltn | grep -E '127.0.0.1:(6443|3000)'`. A malformed `PORT_FORWARD_<n>` value
(for example DNS form `tempo.monitoring.svc:3200:3200`) fails at parse time,
which takes down the whole daemon including `6443`; a crash-looping unit shows
`activating (auto-restart)` in `systemctl --user status`.

## Gotchas

- **A port-forward 401 while the tunnel is green is almost never the key.** The
  relay is a raw SSH tunnel with no Kubernetes auth, so `kubectl get --raw
  /readyz` returns `ok` while every managed forward fails. `kubernetes` 36.0.0
  writes the bearer token to `api_key['authorization']` but reads
  `api_key['BearerToken']`, so requests go out with no `Authorization` header.
  `rotate_session.py` mirrors the token across and re-arms
  `refresh_api_key_hook` on every refresh (a one-shot copy works for one token
  lifetime, about 25 minutes, then 401s again). Diagnose by printing
  `Configuration.auth_settings()`: empty is the tell. The library cannot simply be
  bumped, because `oci-cli` caps `PyYAML<=6.0.2` and that makes 36.0.0 the newest
  resolvable `kubernetes` (see the comment in `pyproject.toml`).
- **Bastion sessions are eventually consistent.** A session reports `ACTIVE`
  several seconds before its registered key is accepted at the SSH endpoint, so an
  immediate ssh gets a transient `Permission denied (publickey)`. The rotator
  settles and retries ssh on the same session; recreating it just re-races the
  window.
- **`IdentitiesOnly=yes`** on the rotator's ssh: a session accepts exactly one
  key, so ssh offers only the `-i` key rather than spraying agent identities and
  hitting `MaxAuthTries`.
- **`StrictHostKeyChecking=accept-new`**: the bastion host key is trusted on first
  use, which is fine for automation against a stable bastion hostname.
