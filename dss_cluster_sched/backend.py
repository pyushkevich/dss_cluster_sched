"""Cluster backends. The scheduler only needs to submit, cancel, re-attach to and
query the state of jobs; PsijBackend does this through psij-python (SLURM, LSF,
local, ...), FakeBackend in memory for tests."""
import logging
import pathlib
import threading
from typing import Dict, Optional

from .state import PENDING, RUNNING

log = logging.getLogger(__name__)

DONE = 'done'


class Backend:
    name = 'backend'

    def submit(self, spec) -> str:
        """Submit a psij JobSpec; return the native job id."""
        raise NotImplementedError

    def attach(self, native_id: str):
        """Resume tracking a job submitted by an earlier scheduler process."""
        raise NotImplementedError

    def state(self, native_id: str) -> Optional[str]:
        """PENDING, RUNNING, DONE, or None if not known yet."""
        raise NotImplementedError

    def cancel(self, native_id: str):
        raise NotImplementedError

    @property
    def supports_attach(self) -> bool:
        return True


class PsijBackend(Backend):

    def __init__(self, executor_name: str, work_directory: str, poll_interval: int = 30):
        import psij
        from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutorConfig
        self.psij = psij
        self.name = executor_name
        work = pathlib.Path(work_directory)
        if executor_name == 'local':
            config = psij.JobExecutorConfig(work_directory=work)
        else:
            # The SLURM and LSF config classes add nothing to the batch base
            # class; queue_polling_interval sets how often psij runs squeue/bjobs.
            config = BatchSchedulerExecutorConfig(work_directory=work,
                                                  queue_polling_interval=poll_interval)
        self.executor = psij.JobExecutor.get_instance(executor_name, config=config)
        self.jobs: Dict[str, object] = {}
        self.lock = threading.Lock()

    @property
    def supports_attach(self) -> bool:
        # psij's local executor can only attach to processes that are still
        # running, and our local jobs die with the scheduler anyway
        return self.name != 'local'

    def submit(self, spec) -> str:
        job = self.psij.Job(spec)
        self.executor.submit(job)
        native_id = str(job.native_id)
        with self.lock:
            self.jobs[native_id] = job
        return native_id

    def attach(self, native_id: str):
        job = self.psij.Job()
        self.executor.attach(job, native_id)
        with self.lock:
            self.jobs[native_id] = job

    def state(self, native_id: str) -> Optional[str]:
        with self.lock:
            job = self.jobs.get(native_id)
        if job is None:
            return None
        st = job.status.state
        if st.final:
            return DONE
        if st == self.psij.JobState.ACTIVE:
            return RUNNING
        if st == self.psij.JobState.QUEUED:
            return PENDING
        return None   # NEW: not polled yet (e.g. just attached)

    def cancel(self, native_id: str):
        with self.lock:
            job = self.jobs.get(native_id)
        if job is not None:
            job.cancel()

    def forget(self, native_id: str):
        with self.lock:
            self.jobs.pop(native_id, None)


class FakeBackend(Backend):
    """In-memory backend for tests: jobs stay in whatever state the test sets."""
    name = 'fake'

    def __init__(self):
        self.jobs: Dict[str, dict] = {}
        self.next_id = 1000
        self.fail_submit = False

    def submit(self, spec) -> str:
        if self.fail_submit:
            raise RuntimeError('submission failed (fake)')
        self.next_id += 1
        nid = str(self.next_id)
        self.jobs[nid] = {'spec': spec, 'state': PENDING, 'cancelled': False}
        return nid

    def attach(self, native_id: str):
        self.jobs.setdefault(native_id, {'spec': None, 'state': None, 'cancelled': False})

    def state(self, native_id: str) -> Optional[str]:
        j = self.jobs.get(native_id)
        return None if j is None else j['state']

    def cancel(self, native_id: str):
        j = self.jobs[native_id]
        j['cancelled'] = True
        j['state'] = DONE

    def forget(self, native_id: str):
        pass


def make_backend(cfg) -> Backend:
    return PsijBackend(cfg.executor, work_directory='%s/psij' % cfg.state_dir,
                       poll_interval=max(1, int(cfg.poll_interval)))
