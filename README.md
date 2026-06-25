# oci-bastion-keepalive

Keep a long-lived OCI **bastion** port-forward to a **private** OKE (Kubernetes)
API server alive across the bastion session's hard TTL — so `kubectl` and
anything else pinned to `127.0.0.1:6443` stop dropping on long sessions.

A bastion port-forwarding session has a maximum TTL. When it expires the SSH
tunnel dies and everything pointed at the local port breaks until you recreate
it by hand. `rotate_session.py` removes both the drop and the manual step:

```
kubectl / clients -> 127.0.0.1:6443   (relay: bound once, never released)
                            |  forwards new connections to the live tunnel
              ssh A -> :7001  ...  ssh B -> :7002   (alternate each rotation)
                            |
                       bastion session -> OKE API :6443
```

A small TCP relay binds `127.0.0.1:6443` for the daemon's whole life and never
releases it — **your kubeconfig points at it once and is never edited again.**
The actual bastion session + SSH tunnel live on internal ports and rotate a few
minutes before each expiry: create the new session → wait `ACTIVE` → start SSH on
the alternate port → health-check (TLS handshake to the API) → flip the relay's
upstream → drain old connections → tear the old session down. The front socket on
6443 is never closed, so the endpoint never disappears. If the SSH tunnel dies
early, it rotates immediately instead of waiting for the timer.

## Requirements

- Python 3.10+ (the [OCI Python SDK](https://pypi.org/project/oci/) is pulled in
  as a dependency on install)
- A configured OCI profile (`~/.oci/config`) with permission to manage bastion
  sessions and read the target cluster
- `ssh` on `PATH`, and an SSH keypair registered for bastion sessions

## Install

```sh
python -m venv venv && . venv/bin/activate
pip install .          # installs the `oci-bastion-keepalive` command + deps
```

## Usage

```sh
export OKE_CLUSTER_OCID=ocid1.cluster.oc1...
export OKE_BASTION_OCID=ocid1.bastion.oc1...
oci-bastion-keepalive          # or: python rotate_session.py
```

Point your kubeconfig at `https://127.0.0.1:6443` and leave the daemon running.
Ctrl-C cleans up the active bastion session on exit.

### Run it unattended (systemd --user)

`rotate-session.service` is a `systemd --user` unit. Copy
`rotate_session.env.example` to `~/.config/oke-rotator/rotate_session.env`, fill
in your OCIDs, then:

```sh
cp rotate-session.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now rotate-session
```

(Adjust the `WorkingDirectory`/`ExecStart` paths in the unit if your checkout and
venv live somewhere other than `~/Code/oci-bastion-keepalive`.)

## Configuration

Everything is environment-driven — no OCIDs or tuning are baked in. Only
`OKE_CLUSTER_OCID` and `OKE_BASTION_OCID` are required; the requested session TTL
is capped to the bastion's maximum. See
[`rotate_session.env.example`](rotate_session.env.example) for the full list of
optional knobs (region, profile, TTL, rotation lead time, ports, SSH key paths,
health-check timeouts, …) with their defaults.

## Managed port-forwards

The daemon can also hold local port-forwards to in-cluster workloads alongside
the API relay — so the ones you'd otherwise open by hand stop going stale
(`error: lost connection to pod`) on a pod restart or a tunnel rotation. Each is
configured with an indexed env var (gaps allowed):

```sh
PORT_FORWARD_1=monitoring/svc/grafana:3000:3000
PORT_FORWARD_2=monitoring/svc/mimir:9009:9009
```

The value is `<namespace>/<kind>/<name>:<localPort>:<remotePort>`, where `<kind>`
is `svc`/`service`, `pod`, or `deploy`/`deployment`. Forwarding is done
in-process via the Kubernetes Python client (no `kubectl` subprocess); the
client rides this same `127.0.0.1` relay and authenticates with the OCI
exec-plugin from your kubeconfig (`KUBECONFIG`, default `~/.kube/config`;
optional `KUBE_CONTEXT`). That plugin runs the `oci` CLI to mint a token, so the
CLI is bundled as a dependency and the daemon adds its own venv `bin` directory
to `PATH` — it resolves with no extra setup, including under `systemd`.

Like the API relay, each local port is **bound once and held for the daemon's
life**, and a fresh pod is resolved on every new connection — so a restarted pod
or a rotated bastion tunnel is picked up automatically, with no manual re-run.
With no `PORT_FORWARD_*` set the feature is dormant.
