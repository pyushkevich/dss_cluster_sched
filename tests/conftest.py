import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, 'testing'))

from dss_testenv import DSSTestServer, setup_environment  # noqa: E402

DUMMY_JOB = os.path.join(REPO, 'testing', 'dummy_job.py')


@pytest.fixture(scope='module')
def dss(tmp_path_factory):
    """A live DSS server with provider 'testprov' serving Dummy-AtlasA/B."""
    work = tmp_path_factory.mktemp('dss')
    server = DSSTestServer(str(work)).start(log_path=str(work / 'server.log'))
    env = setup_environment(server, token_dir=str(work / 'tokens'))
    env['server'] = server
    # Tokens work once: log the provider in here and share the session file
    from itksnap_dss import DSSClient
    from dss_cluster_sched.session import token_login, write_session
    env['session_file'] = str(work / 'provider.session')
    write_session(env['session_file'],
                  token_login(DSSClient(server.base_url), env['tokens']['provider']))
    yield env
    server.stop()


@pytest.fixture(autouse=True)
def _clean_tickets(request):
    """Tests in a module share one server: close out leftover open tickets so
    each test starts with an empty queue."""
    yield
    if 'dss' in request.fixturenames:
        server = request.getfixturevalue('dss')['server']
        server.sql("update tickets set status='deleted' where status in ('init','ready','claimed')")


def make_config(dss, tmp_path, **over):
    """A scheduler config dict for the dss fixture; override keys with **over."""
    cfg = {
        'instance': 'testsched',
        'executor': 'local',
        'poll_interval': 1,
        'claim_timeout': 60,
        'state_dir': str(tmp_path / 'state'),
        'service_refresh': 0,
        'limits': {'max_jobs': 10, 'max_queued': 10},
        'servers': {'local': {'url': dss['server'].base_url,
                              'session_file': dss['session_file'],
                              'provider': dss['provider']}},
        'jobs': {'dummy': {
            'command': '%s %s --ticket {ticket} --atlas {atlas} --duration {duration} --fail {fail}'
                       % (sys.executable, DUMMY_JOB),
            'defaults': {'atlas': 'none', 'duration': '0.2', 'fail': 'no'},
        }},
        'services': {'atlas-a': {'name': 'Dummy-AtlasA', 'job': 'dummy', 'params': {'atlas': 'A'}},
                     'atlas-b': {'name': 'Dummy-AtlasB', 'job': 'dummy', 'params': {'atlas': 'B'}}},
    }
    for k, v in over.items():
        cfg[k] = v
    return cfg
