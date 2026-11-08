"""Scheduler logic against a live DSS server, with an in-memory cluster backend
(FakeBackend) whose job states the tests set by hand."""
import os
import time

from conftest import make_config

from dss_cluster_sched.backend import DONE, FakeBackend
from dss_cluster_sched.config import parse_config
from dss_cluster_sched.scheduler import Scheduler
from dss_cluster_sched.state import RUNNING


def scheduler(dss, tmp_path, backend=None, **over):
    cfg = parse_config(make_config(dss, tmp_path, **over))
    s = Scheduler(cfg, backend or FakeBackend())
    s.restore()
    return s


def tickets(dss, service, n):
    return [dss['server'].create_ticket(dss['consumer'], dss['githashes'][service])
            for _ in range(n)]


def latest_claim(dss, ticket):
    rows = dss['server'].sql('select * from claim_history where ticket_id=? order by id desc',
                             (ticket,))
    return rows[0] if rows else None


def test_claims_are_temporary_and_jobs_rendered(dss, tmp_path):
    s = scheduler(dss, tmp_path, claim_timeout=90)
    [ta] = tickets(dss, 'Dummy-AtlasA', 1)
    [tb] = tickets(dss, 'Dummy-AtlasB', 1)
    assert s.cycle() == 2

    for t, atlas in ((ta, 'A'), (tb, 'B')):
        claim = latest_claim(dss, t)
        assert claim['provider'] == 'testprov' and claim['provider_code'] == 'testsched'
        assert time.time() + 80 < claim['expires'] <= time.time() + 90
        assert dss['server'].ticket_status(t) == 'claimed'
        [rec] = s.state.for_ticket('local', t)
        spec = s.backend.jobs[rec.native_id]['spec']
        assert spec.name == 'dss.testsched.local.%d' % t
        import json
        job = json.load(open(rec.spec_path))
        assert '--ticket %d --atlas %s ' % (t, atlas) in job['command']
        assert job['variables']['service_githash'] == dss['githashes']['Dummy-Atlas' + atlas]
        assert any('Queued as fake job' in m for m in dss['server'].ticket_log(t))

    # Nothing left to claim
    assert s.cycle() == 0


def test_limits_respected(dss, tmp_path):
    s = scheduler(dss, tmp_path, limits={'max_jobs': 5, 'max_queued': 2})
    tickets(dss, 'Dummy-AtlasA', 4)
    tickets(dss, 'Dummy-AtlasB', 4)

    # max_queued=2: two jobs, one per service (round robin)
    assert s.cycle() == 2
    assert sorted(r.service for r in s.state.all()) == ['atlas-a', 'atlas-b']

    # Jobs start running: room for two more queued
    for nid in s.backend.jobs:
        s.backend.jobs[nid]['state'] = RUNNING
    assert s.cycle() == 2
    # max_jobs=5 now allows only one more, once the queued ones start
    for nid in s.backend.jobs:
        s.backend.jobs[nid]['state'] = RUNNING
    assert s.cycle() == 1
    assert s.cycle() == 0
    assert len(s.state.all()) == 5

    # Finished jobs free their slots; job 1005 still pending, so max_queued=2
    # allows only one more pending job
    for nid in list(s.backend.jobs)[:2]:
        s.backend.jobs[nid]['state'] = DONE
    assert s.cycle() == 1
    assert sorted(r.state for r in s.state.all()) == ['pending', 'pending', 'running', 'running']


def test_per_service_and_job_limits(dss, tmp_path):
    cfg = make_config(dss, tmp_path)
    cfg['services']['atlas-a']['limits'] = {'max_jobs': 1}
    cfg['jobs']['dummy']['limits'] = {'max_jobs': 3}
    s = Scheduler(parse_config(cfg), FakeBackend())
    tickets(dss, 'Dummy-AtlasA', 3)
    tickets(dss, 'Dummy-AtlasB', 3)
    assert s.cycle() == 3
    assert sorted(r.service for r in s.state.all()) == ['atlas-a', 'atlas-b', 'atlas-b']


def test_lapsed_claim_reclaimed_without_duplicate_job(dss, tmp_path):
    s = scheduler(dss, tmp_path)
    [t] = tickets(dss, 'Dummy-AtlasA', 1)
    assert s.cycle() == 1
    [rec] = s.state.all()

    # The job is still queued when its claim lapses
    dss['server'].sql("update claim_history set expires = strftime('%s','now') - 1 "
                      "where ticket_id = ?", (t,))
    assert s.cycle() == 0
    assert dss['server'].ticket_status(t) == 'claimed'          # re-claimed for the queued job
    assert latest_claim(dss, t)['expires'] > time.time()
    assert [r.native_id for r in s.state.all()] == [rec.native_id]
    assert len(s.backend.jobs) == 1


def test_pending_job_for_finished_ticket_cancelled(dss, tmp_path):
    s = scheduler(dss, tmp_path, claim_timeout=1)
    [t] = tickets(dss, 'Dummy-AtlasA', 1)
    assert s.cycle() == 1
    [rec] = s.state.all()
    time.sleep(1.1)

    # Ticket still open: the stale pending job stays queued
    s.cycle()
    assert not s.backend.jobs[rec.native_id]['cancelled']

    # Ticket deleted by the user: cancelled, then forgotten
    dss['consumer'].get('/api/tickets/%d/delete' % t)
    assert dss['server'].ticket_status(t) == 'deleted'
    s.cycle()
    assert s.backend.jobs[rec.native_id]['cancelled']
    s.cycle()
    assert s.state.all() == []


def test_submit_failure_does_not_claim_everything(dss, tmp_path):
    backend = FakeBackend()
    backend.fail_submit = True
    s = scheduler(dss, tmp_path, backend=backend)
    ts = tickets(dss, 'Dummy-AtlasA', 3)
    assert s.cycle() == 0
    statuses = [dss['server'].ticket_status(t) for t in ts]
    assert statuses.count('claimed') == 1 and statuses.count('ready') == 2
    claimed = ts[statuses.index('claimed')]
    assert any('could not queue a job' in m for m in dss['server'].ticket_log(claimed))


def test_restart_resumes_tracking(dss, tmp_path):
    backend = FakeBackend()
    s = scheduler(dss, tmp_path, backend=backend, limits={'max_jobs': 2})
    tickets(dss, 'Dummy-AtlasA', 3)
    assert s.cycle() == 2

    # A new scheduler process with the same state dir counts the existing jobs
    backend2 = FakeBackend()
    s2 = scheduler(dss, tmp_path, backend=backend2, limits={'max_jobs': 2})
    assert len(s2.state.all()) == 2
    assert set(backend2.jobs) == set(backend.jobs)
    for nid in backend2.jobs:
        backend2.jobs[nid]['state'] = RUNNING   # first poll after attach
    assert s2.cycle() == 0


def test_dry_run_claims_nothing(dss, tmp_path, capsys):
    s = scheduler(dss, tmp_path)
    [t] = tickets(dss, 'Dummy-AtlasA', 1)
    assert s.cycle(dry_run=True) == 1
    assert 'atlas-a (Dummy-AtlasA): 1 ready ticket(s); would claim' in capsys.readouterr().out
    assert dss['server'].ticket_status(t) == 'ready'
    assert s.state.all() == []


def test_service_pinned_by_githash_and_unreachable_server(dss, tmp_path):
    cfg = make_config(dss, tmp_path)
    cfg['servers']['down'] = {'url': 'http://127.0.0.1:9', 'session_file': dss['session_file'],
                              'provider': 'testprov'}
    cfg['services'] = {'pinned': {'githash': dss['githashes']['Dummy-AtlasB'], 'job': 'dummy',
                                  'params': {'atlas': 'B'}}}
    s = Scheduler(parse_config(cfg), FakeBackend())
    for conn in s.conns.values():
        conn.client.max_attempts = 1          # don't spend 30 s retrying the dead server
    tickets(dss, 'Dummy-AtlasA', 1)
    [tb] = tickets(dss, 'Dummy-AtlasB', 1)
    assert s.cycle() == 1
    assert [r.ticket for r in s.state.all()] == [tb]


def test_token_bootstrap_creates_session(dss, tmp_path):
    """With no session yet, a one-time token_file creates and saves one."""
    srv = dss['server']
    srv.seed_user('provider2@example.com', 'provider2-token-0123456789')
    srv.post(dss['admin'], '/api/admin/providers/testprov/users',
             email='provider2@example.com', admin='0')
    token_file = tmp_path / 'p2.token'
    token_file.write_text('provider2-token-0123456789\n')
    cfg = make_config(dss, tmp_path)
    cfg['servers']['local'] = {'url': srv.base_url, 'provider': 'testprov',
                               'token_file': str(token_file)}
    s = Scheduler(parse_config(cfg), FakeBackend())
    [t] = tickets(dss, 'Dummy-AtlasA', 1)
    assert s.cycle() == 1
    session_file = s.cfg.servers['local'].session_file
    assert os.stat(session_file).st_mode & 0o777 == 0o600
    assert latest_claim(dss, t)['puser_id'] == srv.sql(
        "select id from users where email='provider2@example.com'")[0]['id']

    # The token is now spent, but a new scheduler reuses the saved session
    s2 = Scheduler(parse_config(cfg), FakeBackend())
    tickets(dss, 'Dummy-AtlasA', 1)
    assert s2.cycle() == 1
