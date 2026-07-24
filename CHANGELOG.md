# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.9] - 2026-07-24

### Changed

- fix(deps): update oci python packages

## [0.2.8] - 2026-07-17

### Changed

- Bumped kubernetes to v36.0.3

## [0.2.7] - 2026-07-17

### Changed

- fix(deps): update oci python packages

## [0.2.6] - 2026-07-10

### Changed

- Bumped kubernetes to v36

## [0.2.5] - 2026-07-10

### Changed

- fix(deps): update oci python packages

## [0.2.4] - 2026-07-03

### Changed

- Bumped oci to v2.181.0

## [0.2.3] - 2026-07-03

### Changed

- chore(deps): update https://gitlab.com/tnoff-projects/github-workflows digest to 61f9268

## [0.2.2] - 2026-06-29

### Changed

- chore(deps): update python docker tag to v3.14

## [0.2.1] - 2026-06-28

### Changed

- chore(deps): update https://gitlab.com/tnoff-projects/github-workflows digest to c3e8e55

## [0.2.0] - 2026-06-25

### Added

- Managed port-forwards: the daemon can now hold local port-forwards to
  in-cluster workloads alongside the API relay, configured with indexed
  `PORT_FORWARD_<n>` env vars (`<namespace>/<kind>/<name>:<localPort>:<remotePort>`).
  Forwarding is done in-process via the Kubernetes Python client — no `kubectl`
  subprocess — and each local port is bound for the daemon's life, reconnecting
  to a fresh pod per connection so it self-heals across pod restarts and tunnel
  rotations (no more manual `kubectl port-forward` re-runs). Adds `kubernetes`
  and `oci-cli` dependencies (the kubeconfig exec-plugin shells out to the `oci`
  CLI for a token; the daemon prepends its venv `bin` to `PATH` so it resolves
  under `systemd` too); new `KUBECONFIG` / `KUBE_CONTEXT` knobs.

## [0.1.0] - 2026-06-21

### Added

- `rotate_session.py` — self-rotating OCI bastion port-forward to a private OKE
  API server: a fixed local relay on `127.0.0.1:6443` with the bastion session +
  SSH tunnel rotating underneath it before each hard-TTL expiry.
- `rotate-session.service` / `rotate_session.env.example` — `systemd --user` unit
  and config template for running it unattended.
- pip packaging (`pyproject.toml`) with the `oci-bastion-keepalive` console
  script, and Renovate + auto-tagging CI.
