"""End-to-end tests on the container's SLURM cluster and DSS server.
Run with `docker compose run --rm cluster test` (as user dss, after bootstrap)."""
import os
import signal
import subprocess
import sys
import time

import pytest
import yaml

sys.path.insert(0, '/opt/src/testing')
sys.path.insert(0, '/opt/src/docker')
from dss_testenv import DSSTestServer  # noqa: E402
from submit_tickets import consumer, submit  # noqa: E402

E2E = '/home/dss/e2e'
INSTANCE = 'docker'


@pytest.fixture
def dss():
    c, info = consumer()
    server = DSSTestServer('/home/dss/dss-server', port=8080)
    yield c, info, server
    # Close out tickets and jobs left by the test
    subprocess.run(['scancel', '--me', '--quiet'], check=False)
    server.sql("update tickets set status='deleted' where status in ('init','ready','claimed')")
    partition('UP')


def partition(state):
    subprocess.run(['sudo', 'scontrol', 'update', 'PartitionName=debug', 'State=%s' % state],
                   check=True)


def squeue():
    """Our SLURM jobs: list of (jobid, ticket, state)."""
    out = subprocess.run(['squeue', '--me', '-h', '-o', '%i|%j|%T'], check=True,
                         capture_output=True, text=True).stdout
    jobs = []
    for line in out.splitlines():
        jid, name, st = line.split('|')
        parts = name.split('.')
        if len(parts) == 4 and parts[0] == 'dss' and parts[1] == INSTANCE:
            jobs.append((jid, int(parts[3]), st))
    return jobs


@pytest.fixture
def scheduler(tmp_path):
    """Start the scheduler with the bootstrap config, modified by the test."""
    procs = []

    def start(**over):
        cfg = yaml.safe_load(open(E2E + '/config.yaml'))
        cfg['state_dir'] = str(tmp_path / 'state')
        cfg['poll_interval'] = 2
        cfg.update(over)
        path = tmp_path / 'config.yaml'
        path.write_text(yaml.safe_dump(cfg))
        log = open(tmp_path / 'scheduler.log', 'w')
        p = subprocess.Popen(['dss-cluster-sched', 'run', str(path)], stdout=log,
                             stderr=subprocess.STDOUT)
        procs.append((p, tmp_path / 'scheduler.log'))
        return cfg

    yield start
    for p, log in procs:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
        print('---- scheduler log ----\n' + open(log).read())


def wait_for(cond, timeout, what, interval=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = cond()
        if v:
            return v
        time.sleep(interval)
    raise AssertionError('timed out waiting for ' + what)


def test_tickets_complete_within_limits(dss, scheduler):
    c, info, server = dss
    g = info['githashes']
    service_of = {}
    for name, svc, n in (('Dummy-AtlasA', 'atlas-a', 5), ('Dummy-AtlasB', 'atlas-b', 5),
                         ('Dummy-Slow', 'slow', 2)):
        for _ in range(n):
            service_of[submit(c, g[name])] = svc
    cfg = scheduler()
    lim = cfg['limits']

    max_seen = 0
    deadline = time.time() + 400
    while True:
        jobs = squeue()
        pending = [j for j in jobs if j[2] == 'PENDING']
        per_svc = {}
        for _, t, _ in jobs:
            per_svc[service_of.get(t)] = per_svc.get(service_of.get(t), 0) + 1
        assert len(jobs) <= lim['max_jobs'], jobs
        assert len(pending) <= lim['max_queued'], jobs
        assert per_svc.get('atlas-b', 0) <= 2, jobs
        assert per_svc.get('slow', 0) <= 1, jobs
        assert len({t for _, t, _ in jobs}) == len(jobs), 'two jobs for one ticket: %r' % jobs
        max_seen = max(max_seen, len(jobs))
        statuses = [server.ticket_status(t) for t in service_of]
        if all(s == 'success' for s in statuses):
            break
        assert 'failed' not in statuses
        assert time.time() < deadline, 'tickets not done: %r' % statuses
        time.sleep(0.5)

    assert max_seen == lim['max_jobs']   # the cluster was kept busy
    for t, svc in service_of.items():
        log = server.ticket_log(t)
        assert any(m.startswith('Queued as slurm job') for m in log), log
        if svc in ('atlas-a', 'atlas-b'):
            atlas = '/atlases/' + svc[-1].upper()
            assert any('atlas=%s ' % atlas in m for m in log), log


def test_queued_job_keeps_ticket_after_claim_lapses(dss, scheduler):
    c, info, server = dss
    partition('DOWN')
    t = submit(c, info['githashes']['Dummy-AtlasA'])
    scheduler(claim_timeout=6)
    [(jid, _, st)] = wait_for(lambda: [j for j in squeue() if j[1] == t], 30, 'job submitted')
    assert st == 'PENDING'

    # Several claim periods pass while the job is stuck in the queue
    time.sleep(15)
    assert [j[0] for j in squeue() if j[1] == t] == [jid]      # no duplicate job
    claim = server.sql('select * from claim_history where ticket_id=? order by id desc', (t,))[0]
    assert claim['provider_code'] == INSTANCE
    assert server.ticket_status(t) in ('claimed', 'ready')
    log = server.ticket_log(t)
    assert 'Temporary claim expired, ticket returned to the queue' in log, log
    # ... and the scheduler re-claimed it for the queued job each time
    assert sum(m.startswith('Ticket temporarily claimed by provider testprov instance docker')
               for m in log) >= 2, log

    # The queue frees up: the job takes the ticket and completes it
    partition('UP')
    wait_for(lambda: server.ticket_status(t) == 'success', 120, 'ticket success')
    claim = server.sql('select * from claim_history where ticket_id=? order by id desc', (t,))[0]
    assert claim['expires'] is None


def test_pending_job_cancelled_when_ticket_deleted(dss, scheduler):
    c, info, server = dss
    partition('DOWN')
    t = submit(c, info['githashes']['Dummy-AtlasB'])
    scheduler(claim_timeout=4)
    wait_for(lambda: [j for j in squeue() if j[1] == t], 30, 'job submitted')
    c.get('/api/tickets/%d/delete' % t).raise_for_status()
    assert server.ticket_status(t) == 'deleted'
    wait_for(lambda: not [j for j in squeue() if j[1] == t], 30, 'pending job cancelled')
