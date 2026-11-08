"""Create tickets as the test consumer: submit_tickets.py SERVICE [N]
e.g. `docker compose exec -u dss cluster python /opt/src/docker/submit_tickets.py Dummy-AtlasA 5`"""
import json
import sys

import httpx

E2E = '/home/dss/e2e'


def consumer():
    info = json.load(open(E2E + '/info.json'))
    sid = open(E2E + '/consumer.session').read().strip()
    c = httpx.Client(base_url=info['url'], cookies={'webpy_session_id': sid}, timeout=30)
    return c, info


def submit(c, githash):
    r = c.post('/api/tickets', data={'githash': githash})
    r.raise_for_status()
    t = int(r.text)
    c.post('/api/tickets/%d/status' % t, data={'status': 'ready'}).raise_for_status()
    return t


if __name__ == '__main__':
    c, info = consumer()
    name, n = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 1
    if name not in info['githashes']:
        sys.exit('unknown service %s; choose from %s' % (name, ', '.join(info['githashes'])))
    print(' '.join(str(submit(c, info['githashes'][name])) for _ in range(n)))
