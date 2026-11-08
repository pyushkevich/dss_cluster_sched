"""End to end with real jobs: psij's local executor runs jobwrap + dummy_job.py
as subprocesses, against a live DSS server."""
import json
import time

from conftest import make_config

from dss_cluster_sched import jobwrap
from dss_cluster_sched.backend import PsijBackend
from dss_cluster_sched.config import parse_config
from dss_cluster_sched.scheduler import Scheduler
from dss_cluster_sched.submit import render_job, write_spec_file


def run_until(s, done, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s.cycle()
        if done():
            return
        time.sleep(0.3)
    raise AssertionError('timed out; jobs: %r' % s.state.all())


def make_scheduler(dss, tmp_path, cfg_dict):
    cfg = parse_config(cfg_dict)
    return Scheduler(cfg, PsijBackend('local', '%s/psij' % cfg.state_dir))


def test_jobs_run_and_tickets_succeed(dss, tmp_path):
    srv = dss['server']
    s = make_scheduler(dss, tmp_path, make_config(dss, tmp_path, limits={'max_jobs': 3}))
    ta = [srv.create_ticket(dss['consumer'], dss['githashes']['Dummy-AtlasA']) for _ in range(3)]
    tb = [srv.create_ticket(dss['consumer'], dss['githashes']['Dummy-AtlasB']) for _ in range(3)]
    run_until(s, lambda: all(srv.ticket_status(t) == 'success' for t in ta + tb))
    run_until(s, lambda: not s.state.all())   # finished jobs are forgotten

    for tickets, atlas in ((ta, 'A'), (tb, 'B')):
        for t in tickets:
            log = srv.ticket_log(t)
            assert any('dummy job: atlas=%s ' % atlas in m for m in log), log
            assert any(m.startswith('Processing started on') for m in log)
            # The job's claim is permanent
            claim = srv.sql('select * from claim_history where ticket_id=? order by id desc', (t,))[0]
            assert claim['expires'] is None and claim['provider_code'] == 'testsched'


def test_failing_job_marks_ticket_failed_with_output(dss, tmp_path):
    srv = dss['server']
    cfg = make_config(dss, tmp_path)
    cfg['services']['atlas-a']['params']['fail'] = 'yes'
    s = make_scheduler(dss, tmp_path, cfg)
    t = srv.create_ticket(dss['consumer'], dss['githashes']['Dummy-AtlasA'])
    run_until(s, lambda: srv.ticket_status(t) == 'failed')
    log = srv.ticket_log(t)
    assert 'Job command failed with exit code 3' in log
    att = srv.sql('select A.description from ticket_attachment A where ticket_id=?', (t,))
    assert [a['description'] for a in att] == ['Job output (last 200 lines)']


def test_job_exits_if_ticket_taken_meanwhile(dss, tmp_path, capsys):
    """A job that starts after another provider instance got the ticket for good
    must not run its command."""
    srv = dss['server']
    cfg = parse_config(make_config(dss, tmp_path))
    server = cfg.servers['local']
    githash = dss['githashes']['Dummy-AtlasA']
    t = srv.create_ticket(dss['consumer'], githash)

    # Our temporary claim, then it lapses and another instance claims it for good
    from dss_cluster_sched.dss import DSSConnection
    conn = DSSConnection(server)
    assert conn.claim([githash], cfg.instance, 60)[0] == t
    srv.sql("update claim_history set expires = strftime('%s','now') - 1 where ticket_id=?", (t,))
    assert conn.renew(t, 'other-instance') is True

    spec = render_job(cfg, server, cfg.services['atlas-a'], githash, t)
    spec['command'] = 'touch %s' % (tmp_path / 'ran')
    path = write_spec_file(cfg, server, t, spec)
    assert jobwrap.main([path]) == 0
    assert 'no longer available' in capsys.readouterr().out
    assert not (tmp_path / 'ran').exists()
    assert srv.ticket_status(t) == 'claimed'


def test_job_script_environment(dss, tmp_path):
    cfg_dict = make_config(dss, tmp_path)
    cfg_dict['jobs']['dummy']['environment'] = {
        'env': {'GREETING': 'hi {user}'}, 'setup': 'export FROM_SETUP=1'}
    cfg = parse_config(cfg_dict)
    spec = render_job(cfg, cfg.servers['local'], cfg.services['atlas-b'], 'h' * 40, 42)
    script = jobwrap.build_script(spec)
    assert 'export DSS_TICKET=42\n' in script
    assert 'export DSS_ATLAS=B\n' in script
    assert 'export DSS_SESSION_FILE=' in script
    assert "export GREETING='hi " in script
    assert script.index('GREETING') < script.index('FROM_SETUP') < script.index('exec ')
    assert json.dumps(spec)   # spec file is plain JSON
