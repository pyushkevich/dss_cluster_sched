"""Helpers to stand up a local ITK-SNAP DSS server with a provider, services and
tickets, for the tests and the Docker test environment. Mirrors the approach of
alfabis_server/testing/pytest/conftest.py: users are seeded straight into the
SQLite database and log in with tokens (no OAuth)."""
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time

import httpx


def free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


class DSSTestServer:

    def __init__(self, workdir, port=None):
        self.workdir = os.path.abspath(workdir)
        self.port = port or free_port()
        self.sqlite_path = os.path.join(self.workdir, 'dss.sqlite3')
        self.datastore_root = os.path.join(self.workdir, 'datastore')
        self.base_url = 'http://127.0.0.1:%d' % self.port
        self.proc = None

    # -- process ----------------------------------------------------------------

    def start(self, log_path=None):
        os.makedirs(self.datastore_root, exist_ok=True)
        env = dict(os.environ, ALFABIS_SQLITE_PATH=self.sqlite_path,
                   ALFABIS_DATASTORE_ROOT=self.datastore_root, ALFABIS_COOKIE_DOMAIN='')
        env.pop('ALFABIS_NOAUTH', None)
        out = open(log_path, 'ab') if log_path else subprocess.DEVNULL
        self.proc = subprocess.Popen(
            [sys.executable, '-m', 'itksnap_dss_server', '--server', '--port', str(self.port)],
            cwd=self.workdir, env=env, stdout=out, stderr=subprocess.STDOUT)
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError('DSS server exited with code %d' % self.proc.returncode)
            try:
                httpx.get(self.base_url + '/about', timeout=1)
                return self
            except httpx.TransportError:
                time.sleep(0.1)
        raise RuntimeError('DSS server did not start')

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    # -- database -----------------------------------------------------------------

    def db(self):
        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        return conn

    def sql(self, query, args=()):
        conn = self.db()
        try:
            rows = conn.execute(query, args).fetchall()
            conn.commit()
            return rows
        finally:
            conn.close()

    def seed_user(self, email, token, sysadmin=False, tier='xnat'):
        self.sql('insert into users (email,passwd,dispname,sysadmin,tier) values (?,?,?,?,?)',
                 (email, token, email.split('@')[0], 1 if sysadmin else 0, tier))

    # -- HTTP ---------------------------------------------------------------------

    def login(self, token) -> httpx.Client:
        c = httpx.Client(base_url=self.base_url, timeout=30)
        r = c.post('/api/login', data={'token': token})
        r.raise_for_status()
        assert r.text.startswith('logged in as'), r.text
        return c

    @staticmethod
    def post(client, path, **data):
        r = client.post(path, data=data)
        if r.status_code != 200:
            raise RuntimeError('POST %s -> %d: %s' % (path, r.status_code, r.text))
        return r.text.strip()

    def add_provider(self, admin, name, user_email):
        self.post(admin, '/api/admin/providers', name=name)
        self.post(admin, '/api/admin/providers/%s/users' % name, email=user_email, admin='1')

    def make_service_repo(self, name, version='1.0.0'):
        repo = os.path.join(self.workdir, 'services', '%s-%s' % (name, version))
        os.makedirs(repo, exist_ok=True)
        with open(os.path.join(repo, 'service.json'), 'w') as f:
            json.dump({'name': name, 'version': version, 'shortdesc': 'Test service %s' % name,
                       'longdesc': 'A dummy service for testing dss_cluster_sched',
                       'url': 'https://example.org/%s' % name}, f)
        git = ['git', '-C', repo]
        if not os.path.exists(os.path.join(repo, '.git')):
            subprocess.run(['git', 'init', '-q', '-b', 'main', repo], check=True)
            subprocess.run(git + ['config', 'user.email', 'test@example.com'], check=True)
            subprocess.run(git + ['config', 'user.name', 'Test'], check=True)
        subprocess.run(git + ['add', '-A'], check=True)
        subprocess.run(git + ['commit', '-q', '--allow-empty', '-m', 'service'], check=True)
        return repo

    def add_service(self, admin, provider, name, version='1.0.0') -> str:
        repo = self.make_service_repo(name, version)
        return self.post(admin, '/api/admin/providers/%s/services' % provider, repo=repo, ref='main')

    def create_ticket(self, consumer, githash) -> int:
        tid = int(self.post(consumer, '/api/tickets', githash=githash))
        self.post(consumer, '/api/tickets/%d/status' % tid, status='ready')
        return tid

    def ticket_status(self, ticket) -> str:
        rows = self.sql('select status from tickets where id=?', (ticket,))
        return rows[0]['status'] if rows else None

    def ticket_log(self, ticket):
        return [r['message'] for r in
                self.sql('select message from ticket_log where ticket_id=? order by id', (ticket,))]


def setup_environment(server: DSSTestServer, provider='testprov',
                      services=('Dummy-AtlasA', 'Dummy-AtlasB'), token_dir=None):
    """Seed admin/provider/consumer users, register the provider and services.
    Returns dict with tokens, githashes (by service name) and logged-in clients."""
    tokens = {'admin': 'admin-token-0123456789', 'provider': 'provider-token-0123456789',
              'consumer': 'consumer-token-0123456789'}
    server.seed_user('admin@example.com', tokens['admin'], sysadmin=True)
    server.seed_user('provider@example.com', tokens['provider'])
    server.seed_user('consumer@example.com', tokens['consumer'])
    admin = server.login(tokens['admin'])
    server.add_provider(admin, provider, 'provider@example.com')
    githashes = {name: server.add_service(admin, provider, name) for name in services}
    token_files = {}
    if token_dir:
        os.makedirs(token_dir, exist_ok=True)
        for who, tok in tokens.items():
            p = os.path.join(token_dir, '%s.token' % who)
            with open(p, 'w') as f:
                f.write(tok + '\n')
            os.chmod(p, 0o600)
            token_files[who] = p
    return {'tokens': tokens, 'token_files': token_files, 'githashes': githashes,
            'provider': provider, 'admin': admin, 'consumer': server.login(tokens['consumer'])}
