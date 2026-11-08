# dss-cluster-sched

Runs ITK-SNAP Distributed Segmentation Service (DSS) tickets as jobs on a SLURM
or LSF cluster. One scheduler process, on a login node, polls one or more DSS
servers for tickets of the services it subscribes to, and submits one cluster job
per ticket. Configurable limits cap how many jobs run and how many wait in the
cluster queue.

## How it works

1. Each cycle, the scheduler asks every server which subscribed services have
   ready tickets (`/api/pro/services/available`).
2. While the limits allow, it claims a ticket **temporarily**, for
   `claim_timeout` seconds, and submits a cluster job for it through
   [psij-python](https://exaworks.org/psij-python/).
3. When the job starts, the job wrapper (`python -m dss_cluster_sched.jobwrap`)
   first claims the ticket **for good** (`POST /api/pro/tickets/<id>/claim`):
   - If the temporary claim lapsed while the job was queued, but nobody else took
     the ticket, the job still gets it.
   - If another provider took it, or another job is already working on it, the
     job exits without doing anything.
4. The wrapper then runs your command. If the command fails, or the job is killed,
   while the ticket is still open, the wrapper attaches the last 200 lines of
   output to the ticket and marks it failed.

While a job waits in the cluster queue, its temporary claim may lapse and the
ticket goes back to `ready`. If the same scheduler claims that ticket again, it
does not submit a second job; the queued job keeps it. A queued job is cancelled
only when its ticket is finished or deleted.

Temporary claims need itksnap-dss-server with alfabis_server#12 merged and
itksnap-dss 0.3.0 or later.

## Install

On a file system that the compute nodes can also see:

```bash
python3 -m venv /shared/venvs/dss
/shared/venvs/dss/bin/pip install git+https://github.com/pyushkevich/itksnap-dss-python
/shared/venvs/dss/bin/pip install git+<this repo>
```

`itksnap-dss` is installed from GitHub because 0.3.0, the first version with
temporary claims, is not on PyPI yet.

## Configure

Start from [`examples/config.yaml`](examples/config.yaml), which documents every
key. It has five parts:

- **servers:** DSS servers, each with its URL and the provider name registered there.
- **jobs:** reusable job types. Each one has:
  - `scheduler`: queue, account, duration, cpus, gpus, memory, and `custom`
    psij attributes such as `slurm.constraint` or `lsf.R`;
  - `environment`: `env`, `setup`, `modules` and `venv`, applied in that order;
  - a `command` template and `defaults` for its variables.
- **services:** what to subscribe to. Each one has a DSS service `name` (all its
  current versions) or a pinned `githash`, the `job` that runs it, and `params`.
  Two services can share a job and differ only in their params, e.g. two
  HyperASHS atlases.
- **limits:** `max_jobs` (queued + running) and `max_queued`, set globally and in
  any server, job or service. A ticket is claimed only if every limit that
  applies allows one more job.
- **instance:** identifies this scheduler. It is the provider "code" on DSS and
  part of each job name (`dss.<instance>.<server>.<ticket>`).

Command templates use `{name}` fields. Values come from the built-in variables
first, then the job's `defaults`, then the service's `params`, with later values
overriding earlier ones. The built-ins are `server_url`, `server_key`, `ticket`,
`service_key`, `service_name`, `service_githash`, `job_key`, `provider`,
`provider_code`, `session_file`, `instance` and `user`. Defaults and params can
refer to other variables, e.g. `workdir: /scratch/{user}/{ticket}`. Every
variable is also exported to the job as `DSS_<NAME>`.

```bash
dss-cluster-sched validate config.yaml
```

## Log in

DSS login tokens work **once**: logging in replaces the token. The session it
creates lasts, so log in once per server:

```bash
dss-cluster-sched login config.yaml        # paste a token from <server>/token
```

The session id is saved to `<state_dir>/sessions/<server>.session` with mode 600,
and the jobs reuse it. For unattended setups, a server's `token_file` can hold a
fresh token instead; it is used only when there is no valid session.

## Run

```bash
dss-cluster-sched run config.yaml            # until SIGTERM/SIGINT
dss-cluster-sched run --once config.yaml     # a single cycle
dss-cluster-sched run --dry-run config.yaml  # show what would be claimed
dss-cluster-sched status config.yaml         # jobs being tracked
```

The scheduler records its jobs in `<state_dir>/jobs.json`. After a restart it
re-attaches to them, so the limits still count them. Job specs and logs are kept
under `<state_dir>/jobs` and `<state_dir>/logs`.

## Writing a job

The job command runs on the compute node with these environment variables set:
`DSS_SERVER_URL`, `DSS_TICKET`, `DSS_SESSION_FILE` and the other `DSS_*`
variables. By the time it starts, the ticket is claimed for good. A Python job
gets an authenticated client with:

```python
from dss_cluster_sched.jobwrap import connect
client = connect()        # itksnap_dss.DSSClient using the scheduler's session
client.dssp_download_ticket(ticket, workdir)
...
client.dssp_set_status(ticket, 'success')
```

The job should set the ticket to `success`, or `failed`, itself. If it exits
non-zero without doing so, the wrapper marks the ticket failed.
[`testing/dummy_job.py`](testing/dummy_job.py) is a minimal example.

## Testing

- **Unit and local tests:** these run a real DSS server, and run jobs through
  psij's `local` executor.

  ```bash
  python -m venv .venv && .venv/bin/pip install -e '.[test]'
  .venv/bin/pytest tests
  ```

- **SLURM end to end in Docker:** the container is a single-node SLURM 24.11
  cluster with munge and no cgroups. It also runs a local DSS server with
  provider `testprov` and three dummy services, `Dummy-AtlasA`, `Dummy-AtlasB`
  and `Dummy-Slow`.

  ```bash
  docker compose run --rm cluster test     # docker/test_e2e.py
  docker compose up --build                # run the scheduler; web UI on http://localhost:8080
  docker compose exec -u dss cluster python /opt/src/docker/submit_tickets.py Dummy-AtlasA 5
  docker compose exec -u dss cluster dss-cluster-sched status /home/dss/e2e/config.yaml
  ```

LSF uses the same code path (`executor: lsf`), but has not been tested on a real
LSF cluster.

## Limitations

- psij 0.9.11 ignores `JobSpec.name` in its batch templates. The job name is
  passed as a custom attribute instead: `slurm.job-name` for SLURM, `lsf.J` for LSF.
- A queued job whose ticket was taken by *another* provider is not cancelled
  early; it exits as soon as it starts.
- Bash jobs that call `itksnap-wt` must log in with `itksnap-wt`'s own
  credentials; they cannot reuse the scheduler's session file.
