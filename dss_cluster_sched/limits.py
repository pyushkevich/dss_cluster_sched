"""Limit accounting: global, per-server, per-job-type and per-service caps on the
number of queued (pending) and total (pending + running) jobs."""
from collections import Counter
from typing import Iterable, List, Tuple

from .config import Config, Limits
from .state import PENDING, JobRecord


class LimitTracker:

    def __init__(self, cfg: Config, records: Iterable[JobRecord]):
        self.cfg = cfg
        self.total = Counter()
        self.queued = Counter()
        for r in records:
            self._count(r.server, r.job, r.service, r.state == PENDING)

    @staticmethod
    def _scopes(server, job, service) -> List[Tuple[str, str]]:
        return [('global', ''), ('server', server), ('job', job), ('service', service)]

    def _limits(self, scope, key) -> Limits:
        if scope == 'global':
            return self.cfg.limits
        if scope == 'server':
            return self.cfg.servers[key].limits
        if scope == 'job':
            return self.cfg.jobs[key].limits
        return self.cfg.services[key].limits

    def _count(self, server, job, service, pending, n=1):
        for s in self._scopes(server, job, service):
            self.total[s] += n
            if pending:
                self.queued[s] += n

    def blocking(self, server, job, service):
        """The first limit that prevents one more (pending) job, as a string, or None."""
        for scope, key in self._scopes(server, job, service):
            lim = self._limits(scope, key)
            name = scope if scope == 'global' else '%s %s' % (scope, key)
            if lim.max_jobs is not None and self.total[(scope, key)] >= lim.max_jobs:
                return '%s max_jobs=%d' % (name, lim.max_jobs)
            if lim.max_queued is not None and self.queued[(scope, key)] >= lim.max_queued:
                return '%s max_queued=%d' % (name, lim.max_queued)
        return None

    def can_add(self, server, job, service) -> bool:
        return self.blocking(server, job, service) is None

    def add(self, server, job, service):
        """Account for a newly submitted job, which starts out pending."""
        self._count(server, job, service, True)

    def summary(self) -> str:
        return 'total=%d queued=%d' % (self.total[('global', '')], self.queued[('global', '')])
