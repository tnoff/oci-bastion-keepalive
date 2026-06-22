# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-21

### Added

- `rotate_session.py` — self-rotating OCI bastion port-forward to a private OKE
  API server: a fixed local relay on `127.0.0.1:6443` with the bastion session +
  SSH tunnel rotating underneath it before each hard-TTL expiry.
- `rotate-session.service` / `rotate_session.env.example` — `systemd --user` unit
  and config template for running it unattended.
- pip packaging (`pyproject.toml`) with the `oci-bastion-keepalive` console
  script, and Renovate + auto-tagging CI.
