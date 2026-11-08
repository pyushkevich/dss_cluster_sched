"""The scheduler loop: claim tickets temporarily and submit one cluster job each,
within the configured limits."""
import logging
import os
import time
from typing import Dict, List, Optional

from .backend import DONE, Backend
from .config import Config
from .dss import FINAL_TICKET_STATES, DSSConnection
from .limits import LimitTracker
from .state import PENDING, RUNNING, JobRecord, JobState
from .submit import job_name, psij_job_spec, render_job, write_spec_file

log = logging.getLogger(__name__)

# Outcomes of Scheduler.claim_one
SUBMITTED, DUPLICATE, FAILED = 'submitted', 'duplicate', 'failed'


class Scheduler:

    def __init__(self, cfg: Config, backend: Backend,
                 connections: Optional[Dict[str, DSSConnection]] = None):
        self.cfg = cfg
        self.backend = backend
        self.conns = connections or {k: DSSConnection(s) for k, s in cfg.servers.items()}
        self.state = JobState(os.path.join(cfg.state_dir, 'jobs.json'))
        self.cycle_no = 0
        self.stopping = False

    # -- startup --------------------------------------------------------------

    def restore(self):
        """Reload job records from a previous run and resume tracking them."""
        self.state.load()
        if not self.state.records:
            return
        if not self.backend.supports_attach:
            log.info('Executor %s cannot re-attach to jobs; forgetting %d old job record(s)',
                     self.backend.name, len(self.state.records))
            self.state.records = {}
            self.state.save()
            return
        for rec in self.state.all():
            try:
                self.backend.attach(rec.native_id)
            except Exception as e:
                log.warning('Could not re-attach to job %s (ticket %s/%d): %s; forgetting it',
                            rec.native_id, rec.server, rec.ticket, e)
                self.state.remove(rec.native_id)
        log.info('Resumed tracking %d job(s) from %s', len(self.state.records), self.state.path)
        self.state.save()

    # -- one cycle --------------------------------------------------------------

    def refresh_jobs(self):
        changed = False
        for rec in self.state.all():
            st = self.backend.state(rec.native_id)
            if st is None:
                continue   # not polled yet; keep last known state
            if st == DONE:
                log.info('Job %s for ticket %s/%d finished', rec.native_id, rec.server, rec.ticket)
                self.state.remove(rec.native_id)
                if hasattr(self.backend, 'forget'):
                    self.backend.forget(rec.native_id)
                changed = True
            elif st != rec.state:
                log.info('Job %s for ticket %s/%d is now %s', rec.native_id, rec.server,
                         rec.ticket, st)
                rec.state = st
                changed = True
        if changed:
            self.state.save()

    def cancel_finished_tickets(self):
        """Pending jobs whose claim has lapsed stay queued (when they start, they
        take the ticket if it is still available). But if the ticket has reached a
        final state meanwhile, the job can never do anything useful: cancel it."""
        now = time.time()
        for rec in self.state.all():
            if rec.state != PENDING or now - rec.submit_time < self.cfg.claim_timeout:
                continue
            conn = self.conns[rec.server]
            try:
                status = conn.ticket_status(rec.ticket)
            except Exception as e:
                log.debug('Could not get status of ticket %s/%d: %s', rec.server, rec.ticket, e)
                continue
            if status in FINAL_TICKET_STATES:
                log.info('Ticket %s/%d is %s; cancelling its pending job %s',
                         rec.server, rec.ticket, status, rec.native_id)
                try:
                    self.backend.cancel(rec.native_id)
                except Exception as e:
                    log.warning('Could not cancel job %s: %s', rec.native_id, e)

    def server_order(self) -> List[str]:
        keys = list(self.cfg.servers)
        n = self.cycle_no % len(keys)
        return keys[n:] + keys[:n]

    def claim_and_submit(self, dry_run=False) -> int:
        """Claim tickets and submit jobs while limits allow. Returns the number of
        jobs submitted (or, with dry_run, that would be attempted)."""
        limits = LimitTracker(self.cfg, self.state.all())
        submitted = 0
        for skey in self.server_order():
            if self.stopping:
                break
            conn = self.conns[skey]
            services = [s for s in self.cfg.services.values() if skey in s.servers]
            try:
                hash_to_svc = conn.resolve(services, self.cfg.service_refresh)
                avail = conn.available(list(hash_to_svc))
            except Exception as e:
                log.warning('%s: cannot reach server: %s', skey, e)
                continue

            # Services with ready tickets, in priority order, each with all of
            # its githashes (several versions of a service may be current)
            order: List[str] = []
            for row in avail:
                svc = hash_to_svc.get(row['service'])
                if svc and svc not in order:
                    order.append(svc)
            svc_hashes = {svc: [h for h, s in hash_to_svc.items() if s == svc] for svc in order}

            if dry_run:
                for row in avail:
                    svc = hash_to_svc.get(row['service'])
                    block = limits.blocking(skey, self.cfg.services[svc].job, svc)
                    print('%s: service %s (%s): %s ready ticket(s); %s'
                          % (skey, svc, row['service_name'], row['ready_tickets'],
                             'blocked by ' + block if block else 'would claim'))
                    if not block:
                        submitted += 1
                continue

            # Round-robin over services: one claim each per round
            active = list(order)
            while active and not self.stopping:
                for svc in list(active):
                    job_key = self.cfg.services[svc].job
                    if not limits.can_add(skey, job_key, svc):
                        log.debug('%s/%s: %s', skey, svc, limits.blocking(skey, job_key, svc))
                        active.remove(svc)
                        continue
                    result = self.claim_one(conn, svc, svc_hashes[svc])
                    if result == SUBMITTED:
                        limits.add(skey, job_key, svc)
                        submitted += 1
                    elif result != DUPLICATE:
                        # Nothing to claim, or claiming/submission failed: stop
                        # for this service until the next cycle, so that a broken
                        # submission does not claim every ready ticket.
                        active.remove(svc)
        return submitted

    def claim_one(self, conn: DSSConnection, svc_key: str, githashes: List[str]):
        """Claim one ticket and submit its job. Returns SUBMITTED, DUPLICATE (the
        ticket already has a queued job of ours), FAILED, or None (nothing to claim)."""
        server = conn.server
        try:
            claimed = conn.claim(githashes, self.cfg.instance, self.cfg.claim_timeout)
        except Exception as e:
            log.warning('%s/%s: claim failed: %s', server.key, svc_key, e)
            return FAILED
        if claimed is None:
            return None
        ticket, githash = claimed

        existing = self.state.for_ticket(server.key, ticket)
        if existing:
            # Our job's temporary claim lapsed and we got the ticket again; the
            # queued job will take it when it starts. Don't submit another one.
            log.info('%s: ticket %d already has job %s queued; re-claimed it for that job',
                     server.key, ticket, existing[0].native_id)
            return DUPLICATE

        svc = self.cfg.services[svc_key]
        try:
            spec = render_job(self.cfg, server, svc, githash, ticket)
            spec_path = write_spec_file(self.cfg, server, ticket, spec)
            jspec = psij_job_spec(self.cfg, self.cfg.jobs[svc.job], job_name(self.cfg, server, ticket),
                                  spec_path)
            native_id = self.backend.submit(jspec)
        except Exception as e:
            log.error('%s: could not submit job for ticket %d: %s', server.key, ticket, e)
            conn.log(ticket, 'warning', 'Provider %s could not queue a job for this ticket; '
                     'it will return to the queue shortly' % server.provider)
            return FAILED

        rec = JobRecord(server=server.key, service=svc_key, job=svc.job, ticket=ticket,
                        native_id=native_id, submit_time=time.time(), spec_path=spec_path)
        self.state.add(rec)
        self.state.save()
        log.info('%s: ticket %d (%s) submitted as %s job %s', server.key, ticket, svc_key,
                 self.backend.name, native_id)
        conn.log(ticket, 'info', 'Queued as %s job %s by provider %s instance %s'
                 % (self.backend.name, native_id, server.provider, self.cfg.instance))
        return SUBMITTED

    def cycle(self, dry_run=False) -> int:
        self.refresh_jobs()
        if not dry_run:
            self.cancel_finished_tickets()
        n = self.claim_and_submit(dry_run=dry_run)
        self.cycle_no += 1
        return n

    def run(self, once=False, dry_run=False):
        self.restore()
        while not self.stopping:
            try:
                self.cycle(dry_run=dry_run)
            except Exception:
                log.exception('Scheduler cycle failed')
            if once or dry_run:
                break
            deadline = time.time() + self.cfg.poll_interval
            while not self.stopping and time.time() < deadline:
                time.sleep(min(1.0, deadline - time.time()))
