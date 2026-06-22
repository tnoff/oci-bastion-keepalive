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

- Python 3.9+ and the [OCI Python SDK](https://pypi.org/project/oci/) (`pip install oci`)
- A configured OCI profile (`~/.oci/config`) with permission to manage bastion
  sessions and read the target cluster
- `ssh` on `PATH`, and an SSH keypair registered for bastion sessions

## Usage

```sh
python -m venv venv && . venv/bin/activate
pip install oci

export OKE_CLUSTER_OCID=ocid1.cluster.oc1...
export OKE_BASTION_OCID=ocid1.bastion.oc1...
python rotate_session.py
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
