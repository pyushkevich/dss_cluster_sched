import datetime
import os

import pytest

from dss_cluster_sched.config import (ConfigError, Limits, load_config, parse_config,
                                      parse_duration, parse_memory, render)
from dss_cluster_sched.limits import LimitTracker
from dss_cluster_sched.state import PENDING, RUNNING, JobRecord, JobState

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def base(**over):
    raw = {
        'instance': 'x',
        'servers': {'s1': {'url': 'http://a/', 'provider': 'p'}},
        'jobs': {'j': {'command': 'run --ticket {ticket} --atlas {atlas}',
                       'defaults': {'atlas': 'none'}}},
        'services': {'svc': {'name': 'S', 'job': 'j'}},
    }
    raw.update(over)
    return raw


def test_example_config_loads():
    cfg = load_config(os.path.join(REPO, 'examples', 'config.yaml'))
    assert cfg.jobs['hyperashs'].limits.max_jobs == 8
    assert cfg.services['ashs-pmc-t1'].params['atlas'].endswith('pmc_t1')
    assert cfg.servers['dss'].url == 'https://dss.itksnap.org'


def test_defaults_and_normalisation():
    cfg = parse_config(base())
    assert cfg.servers['s1'].url == 'http://a'
    assert cfg.services['svc'].servers == ['s1']
    assert cfg.claim_timeout == 120
    assert cfg.limits == Limits()
    assert cfg.servers['s1'].session_file == os.path.join(cfg.state_dir, 'sessions', 's1.session')
    assert cfg.servers['s1'].token_file is None


@pytest.mark.parametrize('mutate, message', [
    (lambda r: r['jobs']['j'].update(command='run {nope}'), "Unknown template variable '{nope}'"),
    (lambda r: r['services']['svc'].update(job='other'), 'not one of the configured jobs'),
    (lambda r: r['services']['svc'].update(servers=['zz']), 'not one of the configured servers'),
    (lambda r: r['services']['svc'].update(githash='abc'), "exactly one of 'name' or 'githash'"),
    (lambda r: r.update(bogus=1), 'unknown key(s) bogus'),
    (lambda r: r['jobs']['j'].update(scheduler={'gpu': 1}), 'unknown key(s) gpu'),
    (lambda r: r['jobs']['j'].update(scheduler={'custom': {'constraint': 'x'}}), 'prefixed'),
    (lambda r: r['jobs']['j'].update(scheduler={'duration': 'soon'}), 'Invalid duration'),
    (lambda r: r['jobs']['j'].update(limits={'max_jobs': -1}), 'non-negative'),
    (lambda r: r['jobs']['j'].update(command='run {x.y}'), 'Unsupported template field'),
    (lambda r: r['jobs']['j']['defaults'].update(a='{b}', b='{a}'), None),  # unused cycle is fine
    (lambda r: r['jobs']['j'].update(command='{a}', defaults={'a': '{b}', 'b': '{a}'}), 'cycle'),
])
def test_validation_errors(mutate, message):
    raw = base()
    mutate(raw)
    if message is None:
        parse_config(raw)
        return
    with pytest.raises(ConfigError) as e:
        parse_config(raw)
    assert message in str(e.value)


def test_service_params_override_job_defaults_and_reference_builtins():
    v = {'ticket': 7, 'user': 'u', 'atlas': '/a/{user}', 'workdir': '/scratch/{user}/{ticket}'}
    assert render('x {atlas} {workdir} {{literal}}', v) == 'x /a/u /scratch/u/7 {literal}'


def test_durations_and_memory():
    assert parse_duration(90) == datetime.timedelta(minutes=90)
    assert parse_duration('02:00:00') == datetime.timedelta(hours=2)
    assert parse_duration('1-12:00:00') == datetime.timedelta(days=1, hours=12)
    assert parse_duration('10:30') == datetime.timedelta(minutes=10, seconds=30)
    assert parse_memory('16G') == 16 * 2**30
    assert parse_memory('512M') == 512 * 2**20
    assert parse_memory('1.5GiB') == int(1.5 * 2**30)


def test_limits_all_levels():
    raw = base(limits={'max_jobs': 5, 'max_queued': 2})
    raw['servers']['s1']['limits'] = {'max_jobs': 4}
    raw['jobs']['j']['limits'] = {'max_queued': 1}
    raw['services']['svc2'] = {'name': 'S2', 'job': 'j', 'limits': {'max_jobs': 1}}
    cfg = parse_config(raw)

    def rec(svc, state):
        return JobRecord('s1', svc, 'j', 1, 'n', 0, '', state)

    lt = LimitTracker(cfg, [])
    assert lt.can_add('s1', 'j', 'svc')
    lt.add('s1', 'j', 'svc')                     # one pending job of type j
    assert lt.blocking('s1', 'j', 'svc') == 'job j max_queued=1'

    lt = LimitTracker(cfg, [rec('svc', RUNNING)])
    assert lt.blocking('s1', 'j', 'svc2') is None
    lt = LimitTracker(cfg, [rec('svc2', RUNNING)])
    assert lt.blocking('s1', 'j', 'svc2') == 'service svc2 max_jobs=1'
    lt = LimitTracker(cfg, [rec('svc', RUNNING)] * 4)
    assert lt.blocking('s1', 'j', 'svc') == 'server s1 max_jobs=4'
    raw['servers']['s1']['limits'] = {}
    lt = LimitTracker(parse_config(raw), [rec('svc', RUNNING)] * 5)
    assert lt.blocking('s1', 'j', 'svc') == 'global max_jobs=5'


def test_state_file_round_trip(tmp_path):
    st = JobState(str(tmp_path / 'sub' / 'jobs.json'))
    st.add(JobRecord('s1', 'svc', 'j', 12, '345', 1.5, '/spec.json'))
    st.save()
    st2 = JobState(st.path)
    st2.load()
    assert st2.all() == st.all()
    assert st2.for_ticket('s1', 12)[0].state == PENDING
