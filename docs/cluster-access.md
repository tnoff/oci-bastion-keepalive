# Local cluster access (OCI bastion port-forward)

How `kubectl` (and the in-cluster MCP servers that ride on it) reach the
**private** OKE API server from the laptop, and how to keep that access alive
across long sessions. Companion to the cluster MCP page (mcp-local TechDocs), which assumes "a live
bastion session" exists — this doc is where that session comes from.

## TL;DR

- The OKE API server has only a **private** endpoint. Local `kubectl` reaches
  it through an OCI **bastion port-forwarding session** + an `ssh -L` tunnel
  that lands on `127.0.0.1:6443`. The repo kubeconfig points there.
- `generate_session.sh` is the **one-shot** path: create a session,
  wait `ACTIVE`, print the `ssh -L` command to run by hand.
- A bastion session has a **hard TTL** (capped by the bastion's
  `max_session_ttl_in_seconds`). When it expires the tunnel dies and everything
  pinned to `127.0.0.1:6443` — `kubectl` and all three in-cluster MCPs — drops
  until you regenerate by hand.
- `rotate_session.py` is the **long-lived** path: it holds
  `127.0.0.1:6443` for its whole life and rotates the session+tunnel underneath
  before each expiry, so the endpoint never disappears and the kubeconfig is
  never edited.
- The same daemon optionally holds **managed port-forwards** to in-cluster
  workloads (`PORT_FORWARD_<n>`), bound for its whole life and reconnected per
  request — so hand-run `kubectl port-forward`s stop going stale. See
  *Managed port-forwards* below.
- The rotator and its `systemd` unit live in this repo,
  [`oci-bastion-keepalive`](https://github.com/tnoff/oci-bastion-keepalive)
  (pip-installable as `oci-bastion-keepalive`; see the [README](README.md) for
  install and usage). `generate_session.sh` lives in the `docker-apps` repo
  checkout as a **local, gitignored** one-shot — it carries the cluster +
  bastion OCIDs, so it's never committed.

## Why a bastion at all

The cluster's API endpoint is private (`endpoints.private-endpoint`, a
`10.x.x.x:6443` VCN address), so there's no public IP to hit. The OCI bastion
service brokers a port-forward to that private IP over SSH; the laptop only ever
talks to its own `127.0.0.1:6443`. Auth to the API server is separate — the
kubeconfig uses the OCI exec-plugin (needs the repo `venv` on `PATH`), the same
mechanism the cluster MCP (mcp-local TechDocs) and OCI MCP (mcp-local
TechDocs) pages describe.

## One-shot: `generate_session.sh`

```console
source venv/bin/activate          # OCI exec-plugin on PATH
./generate_session.sh             # prints an `ssh -L 6443:...` command
# run the printed command in another terminal; leave it open
```

The script resolves the API server private IP from
`oci ce cluster get … endpoints.private-endpoint`, creates a port-forwarding
session (TTL `10800`), polls until `ACTIVE`, then prints the SSH command with
`<localPort>` substituted to `6443`. Fine for short tasks; the tunnel dies with
the session and you re-run it manually.

## Long-lived: `rotate_session.py`

Same access path, but self-renewing and continuous. The design solves one
constraint: **the kubeconfig is hardwired to `127.0.0.1:6443`, and two ssh
tunnels can't both bind that port**, so a naive "start the next one alongside
the old" doesn't work.

A small TCP **relay binds `127.0.0.1:6443` once and never releases it**; the
ssh tunnels live on internal ports (`7001`/`7002`, alternating) and rotate
beneath it:

```
kubectl / MCP -> 127.0.0.1:6443   (relay: bound once, never released)
                        |  forwards each new connection to the live tunnel
          ssh A -> :7001   ...   ssh B -> :7002   (alternate per rotation)
                        |
                   bastion session -> OKE API :6443
```

Rotation, fired `ROTATE_LEAD` seconds (default 300) before the active session's
expiry — or immediately if the active ssh process exits early:

1. Create a new session via the OCI **Python SDK**; poll until `ACTIVE`.
2. Settle briefly (`SSH_SETTLE`), then start ssh on the alternate internal port.
   A session reports `ACTIVE` a few seconds *before* its registered key is
   honoured at the SSH endpoint, so ssh is retried against the **same** session
   (`SSH_ATTEMPTS` × `SSH_RETRY`) to ride that window out — recreating the
   session each try just re-races it (see Gotchas).
3. **Health-check** it — HTTPS to the API through the new tunnel (`200`, or a
   `401`/`403`, all prove TLS+HTTP reached the real API server). The active
   tunnel is **never touched until the replacement is proven healthy**.
4. Flip the relay's upstream to the new port. New connections go to it.
5. Sleep `DRAIN_SECONDS` so in-flight connections finish, then kill the old ssh
   and `delete_session` the old session.

If the replacement can't be stood up — a transient OCI API error, a session
that never goes healthy — the rotation is **retried after `ROTATE_RETRY`
without disturbing the active tunnel**: a failed rotation can't take down a
working one, and a transient API blip can't crash the daemon.

The front socket on `6443` is never closed, so `kubectl`/MCP never see
"connection refused." The honest limit: a long-lived stream pinned to the old
tunnel (a `kubectl logs -f`, a watch) still breaks when its session is torn
down — the client just reconnects instantly through the stable port and lands
on the new tunnel. Short request/response calls (the bulk of MCP traffic) sail
through the flip.

### Configuration

All OCIDs and tuning come from the environment — nothing baked into the script
(`OKE_CLUSTER_OCID` + `OKE_BASTION_OCID` required; everything else optional with
defaults). Beyond the basics (`OCI_REGION`, `SESSION_TTL`, `ROTATE_LEAD`,
`DRAIN_SECONDS`, `INTERNAL_PORTS`, `SSH_PRIVATE_KEY`), the resilience knobs are
`ROTATE_RETRY` (wait before re-attempting a failed rotation) and the
SSH-propagation trio `SSH_SETTLE` / `SSH_ATTEMPTS` / `SSH_RETRY`. The requested
TTL is capped to the bastion's `max_session_ttl_in_seconds`. The ssh command is
derived from the session's `ssh-metadata.command`, same as the one-shot script.
Needs the `oci` SDK — `pip install .` from the repo checkout (or `pip install oci`).

### Set-and-forget: `systemd --user`

`rotate-session.service` + `rotate_session.env.example` run it as a user
service — survives logout (with `loginctl enable-linger`), restarts on crash,
logs to journald. It runs **as you**, so `~/.ssh`, `~/.oci`, and the SSH agent
work natively — no secrets to mount (the reason a container was considered and
rejected). `KillSignal=SIGINT` is set so `systemctl --user stop` triggers the
script's cleanup and deletes the active session on the way out.

## Managed port-forwards

The same daemon can also hold local **port-forwards to in-cluster workloads**
alongside the API relay — so the ones you'd otherwise open by hand
(`kubectl port-forward -n monitoring svc/grafana 3000:3000`) stop going stale
(`error: lost connection to pod`) on a pod restart or a tunnel rotation. They're
configured with indexed env vars (gaps allowed):

```
PORT_FORWARD_1=monitoring/svc/grafana:3000:3000
PORT_FORWARD_2=monitoring/svc/mimir:9009:9009
```

The value is `<namespace>/<kind>/<name>:<localPort>:<remotePort>`, where `<kind>`
is `svc`/`service`, `pod`, or `deploy`/`deployment`. Forwarding is **in-process
via the Kubernetes Python client — no `kubectl` subprocess**: the client rides
this same `127.0.0.1` relay, so port-forwards only work once the relay above is
healthy, and a `svc`/`deploy` target is resolved down to a Running+Ready pod.

The mechanism mirrors the relay's. Each local port is **bound once and held for
the daemon's life**, and a fresh pod is resolved on *every new connection* — so
a restarted pod or a rotated bastion session is picked up on the next connection
with no manual re-run, strictly better than a hand-run `kubectl port-forward`
(which exits and stays dead). With no `PORT_FORWARD_*` set the feature is
dormant. `KUBECONFIG` (default `~/.kube/config`) and `KUBE_CONTEXT` select the
config.

The kube exec-plugin mints its token by shelling out to the **`oci` CLI**
(`oci ce cluster generate-token`), so `oci-cli` is bundled as a dependency and
the daemon prepends its own venv `bin` to `PATH` — the token resolves with no
extra setup, including under `systemd` (whose `ExecStart=venv/bin/python` would
not otherwise put `venv/bin` on `PATH`). This was the original cause of
port-forwards failing `401 Unauthorized` with only the OCI *SDK* installed.

## Gotchas worth knowing

- **A port-forward 401 while the tunnel is green is almost never the key.** The
  relay is a raw SSH tunnel and needs no Kubernetes auth, so it stays healthy
  and `kubectl get --raw /readyz` returns `ok` while every managed forward
  fails. `kubectl` is Go and unaffected. The forwards go through the Python
  client, which is the only piece that has to attach a token.

  Hit 2026-09-05: **`kubernetes` 36.0.0 disagrees with itself about where the
  bearer token lives.** `config/kube_config.py` writes
  `api_key['authorization']`; `client/configuration.py::auth_settings` reads
  `api_key['BearerToken']`, finds nothing, and sends the request with **no
  `Authorization` header at all**. The API server returns a clean `401`,
  indistinguishable from an expired credential.

  Diagnose it by printing `Configuration.auth_settings()` — **empty is the
  tell**. Four plausible theories were investigated and discarded first (`oci`
  missing from the daemon's `PATH`, the service not running from its venv, the
  exec `apiVersion` being unsupported, token expiry); each was wrong, and
  reading the installed library source is what settled it in minutes. Note the
  version cannot simply be bumped: `oci-cli` caps `PyYAML<=6.0.2`, which makes
  36.0.0 the newest resolvable `kubernetes`.

  Fixed in `oci-bastion-keepalive` GitLab MRs !12 and !13 by mirroring the value across.
  **!12 alone was not enough**, and the reason generalises: a one-shot copy
  works for exactly one token lifetime and then 401s again — "it worked for a
  bit", about 25 minutes. `get_api_key_with_prefix()` runs
  `refresh_api_key_hook` *before* reading the key, and the loader's hook calls
  `_set_config()`, which rewrites the token **and reassigns the hook to its own
  closure** — so a wrapper that does not reinstall itself is unhooked on first
  refresh. The re-arm line is load-bearing.

- **Bastion sessions are eventually consistent.** A session reports `ACTIVE`
  several seconds before its registered public key is accepted at the SSH
  endpoint, so an immediate ssh gets a transient `Permission denied
  (publickey)`. The rotator settles (`SSH_SETTLE`) and retries ssh against the
  *same* session — recreating it just re-races the same window. This was the
  cause of intermittent rotation/bring-up failures and cost real debugging to
  pin down; the surfaced ssh stderr is what made it visible.
- **`IdentitiesOnly=yes` on the rotator's ssh.** A session accepts exactly one
  key, so ssh must offer only the `-i` key; otherwise a loaded SSH agent sprays
  its other identities first and can hit `MaxAuthTries` before reaching the
  right one. (Necessary hygiene, but *not* the cure for the gotcha above — that
  one is propagation timing, not key selection.)
- **The endpoint's continuity depends on the daemon process.** If
  `rotate_session.py` dies, `6443` goes with it. The `systemd` unit's
  `Restart=on-failure` is what makes that self-healing; the bare foreground run
  does not.
- **Bastion max TTL is the real ceiling, not the script's `SESSION_TTL`.** The
  script reads `max_session_ttl_in_seconds` and caps to it, so a too-large
  request is silently clamped rather than failing `create_session`.
- **The OCI exec-plugin still runs on the host**, next to `kubectl` — not in any
  tunnel. The tunnel only carries the TLS bytes; auth is unaffected by rotation.
- **`StrictHostKeyChecking=accept-new`** on the rotator's ssh — the bastion host
  key is trusted on first use. Fine for automation against a stable bastion
  hostname.

---

## Verified against

| Project | SHA | Date |
|---|---|---|
| `oci-bastion-keepalive` | `53f44a9` | 2026-07-14 |

*Related: cluster MCP (mcp-local TechDocs; the in-cluster MCPs that depend on
this tunnel), local MCP containers (mcp-local TechDocs; running those MCPs
locally over this keepalive, adding the LGTM `PORT_FORWARD_N` service forwards
on top of `6443`), OCI MCP (mcp-local TechDocs; the local MCP, which needs no
cluster access), infra bootstrap (terraform-admin TechDocs; broader OCI/bastion
provisioning context). The [README](README.md) covers install, usage, and a
shorter summary of the rotation and managed port-forward behaviour described
here.*
