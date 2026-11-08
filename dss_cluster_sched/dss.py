"""Connection to one DSS server, as a provider."""
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

import httpx

# DSSClient stores session ids in the system keyring, which does not exist on
# headless cluster nodes; we keep them in session files instead (see session.py).
os.environ.setdefault('PYTHON_KEYRING_BACKEND', 'keyring.backends.null.Keyring')

from itksnap_dss import DSSClient  # noqa: E402

from .config import Server, Service  # noqa: E402
from .session import (LoginError, read_session, session_valid, token_login,  # noqa: E402
                      use_session, write_session)

log = logging.getLogger(__name__)

FINAL_TICKET_STATES = ('success', 'failed', 'timeout', 'deleted')


class DSSConnection:

    def __init__(self, server: Server, client_factory=DSSClient):
        self.server = server
        self.client = client_factory(server.url)
        self.logged_in = False
        self._services_cache = None
        self._services_time = 0.0

    def login(self):
        """Use the saved session; if there is none (or it is no longer valid),
        create one from the server's one-time token_file, if configured."""
        srv = self.server
        sid = read_session(srv.session_file)
        if sid:
            use_session(self.client, sid)
            if session_valid(self.client):
                self.logged_in = True
                return
            log.warning('%s: saved session in %s is no longer valid', srv.key, srv.session_file)
        if srv.token_file and os.path.exists(srv.token_file):
            with open(srv.token_file) as f:
                token = f.read().strip()
            if token and token != getattr(self, '_used_token', None):
                self._used_token = token
                try:
                    sid = token_login(self.client, token)
                except Exception as e:
                    raise LoginError('%s: login with token from %s failed (tokens work only '
                                     'once): %s' % (srv.key, srv.token_file, e))
                write_session(srv.session_file, sid)
                log.info('%s: logged in with token from %s; session saved to %s',
                         srv.key, srv.token_file, srv.session_file)
                self.logged_in = True
                return
        raise LoginError('%s: no valid DSS session; run `dss-cluster-sched login <config> %s`'
                         % (srv.key, srv.key))

    def _call(self, fn, *args, **kwargs):
        """Call a client method, logging in first if needed and again if the
        session has expired."""
        if not self.logged_in:
            self.login()
        try:
            return fn(*args, **kwargs)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                log.info('%s: session rejected, logging in again', self.server.key)
                self.login()
                return fn(*args, **kwargs)
            raise

    # -- services -----------------------------------------------------------

    def provider_services(self, max_age: float) -> List[Dict]:
        """Current services this provider serves on the server: dicts with
        service, version, hash. Cached for max_age seconds."""
        if self._services_cache is None or time.time() - self._services_time > max_age:
            df = self._call(self.client.dssp_list_services)
            rows = [] if df is None else df.to_dict('records')
            self._services_cache = [r for r in rows if str(r['provider']) == self.server.provider]
            self._services_time = time.time()
        return self._services_cache

    def resolve(self, services: List[Service], max_age: float) -> Dict[str, str]:
        """Map githash -> service key for the configured services available on
        this server. A service given by name matches all its current versions."""
        out = {}
        avail = self.provider_services(max_age)
        for svc in services:
            for row in avail:
                if (svc.githash and row['hash'] == svc.githash) or \
                   (svc.name and row['service'] == svc.name):
                    if row['hash'] in out and out[row['hash']] != svc.key:
                        log.warning('%s: service %s (%s) matched by both %s and %s; using %s',
                                    self.server.key, row['service'], row['hash'],
                                    out[row['hash']], svc.key, out[row['hash']])
                        continue
                    out[row['hash']] = svc.key
        return out

    def available(self, githashes: List[str]) -> List[Dict]:
        """Services among githashes with ready tickets, in claim priority order."""
        if not githashes:
            return []
        df = self._call(self.client.dssp_available_services, githashes)
        return [] if df is None else df.to_dict('records')

    # -- tickets ------------------------------------------------------------

    def claim(self, githashes: List[str], provider_code: str,
              timeout: int) -> Optional[Tuple[int, str]]:
        """Temporarily claim the next ready ticket for these service hashes.
        Returns (ticket id, service githash of the ticket), or None."""
        df = self._call(self.client.dssp_claim_ticket, githashes, self.server.provider,
                        provider_code, claim_timeout=timeout)
        if df is None or len(df) == 0:
            return None
        return int(df['ticket'].iloc[0]), str(df['service'].iloc[0])

    def renew(self, ticket: int, provider_code: str, timeout: Optional[int] = None) -> bool:
        return self._call(self.client.dssp_renew_claim, ticket, self.server.provider,
                          provider_code, claim_timeout=timeout)

    def ticket_status(self, ticket: int) -> str:
        r = self._call(self.client.get_, 'api/pro/tickets/%d/status' % ticket)
        return r.text.strip()

    def log(self, ticket: int, category: str, message: str):
        """Post to the ticket log; failures are logged but not raised."""
        try:
            self._call(self.client.dssp_log, ticket, category, message)
        except Exception as e:
            log.warning('%s: could not post to log of ticket %d: %s', self.server.key, ticket, e)
