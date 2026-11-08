# CLAUDE.md

## What this is

`dss-cluster-sched` is a scheduler that turns ITK-SNAP DSS tickets into SLURM/LSF jobs through psij-python. It relies on temporary claims in itksnap-dss-server (alfabis_server#12) and itksnap-dss ≥ 0.3.0 (`dssp_claim_ticket(claim_timeout=)` and `dssp_renew_claim`). Sibling repos: `../alfabis_server` (server) and `../itksnap-dss-python` (client).

## Commands

```bash
python -m venv .venv && .venv/bin/pip install -e '.[test]'   # or -e ../alfabis_server -e ../itksnap-dss-python
.venv/bin/pytest tests                        # host tests: live DSS server subprocess, Fake + psij local backends
docker compose run --rm cluster test          # SLURM end to end (docker/test_e2e.py); source is bind-mounted, no rebuild needed
docker compose up --build                     # scheduler on the container's cluster; DSS web UI on :8080
```

## Architecture

- **Modules:**
  - `config.py`: YAML parsing to dataclasses, validation, `{name}` template rendering. Every service's templates are checked at load time.
  - `scheduler.py`: the cycle, in order `refresh_jobs`, `cancel_finished_tickets`, `claim_and_submit`.
  - `limits.py`: global, server, job and service limits on `max_jobs` and `max_queued`.
  - `state.py`: `jobs.json` of submitted jobs.
  - `submit.py`: builds the spec JSON and the psij `JobSpec`.
  - `jobwrap.py`: runs on the compute node.
- **Backend:** `backend.py` is a thin interface (`submit`, `attach`, `state`, `cancel`). `PsijBackend` wraps psij; `FakeBackend` is for tests. The scheduler never calls psij directly.
- **Auth:** DSS tokens are single-use, because `/api/login` rotates `users.passwd`. Never design anything that logs in with the same token twice. Sessions last for a year. `session.py` stores the session id in a mode-600 file, which the scheduler and all its jobs share. Set `PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring` everywhere, because `DSSClient` touches the keyring.
- **Claim flow:**
  1. The scheduler claims temporarily, with provider code = `instance`.
  2. jobwrap calls `dssp_renew_claim` with the same provider and code. This succeeds for our live temporary claim or for a `ready` ticket, and fails on any permanent claim.
  3. If the scheduler re-claims a ticket that already has a queued job of ours, it does not submit again (`DUPLICATE`).
  4. After a claim or submission failure, the scheduler stops claiming for that service until the next cycle, so a broken backend can't claim every ticket.
- **psij quirks (0.9.11):**
  - `JobSpec.name` is ignored, so the job name goes in custom attributes as `slurm.job-name` or `lsf.J`.
  - The local executor can't re-attach after a restart; records are dropped.
  - Status lags behind the cluster by `queue_polling_interval`, which is set to `poll_interval`.

## Gotchas

- Template fields must be plain `{name}`. Write literal braces as `{{ }}`. Unknown variables or cycles raise `ConfigError` at load time.
- Ticket delete on the server is `GET /api/tickets/<id>/delete`.
- The Docker image uses Debian trixie (SLURM 24.11), because bookworm's 22.05 can't run without cgroups (`CgroupPlugin=disabled` is 23.02+). `entrypoint.sh` generates the node line from `slurmd -C`.
- `testing/dss_testenv.py` is shared by `tests/` and `docker/bootstrap.py`. It seeds users straight into SQLite, as alfabis_server's conftest does.
- Code style: 4-space indentation, no linter configured.
