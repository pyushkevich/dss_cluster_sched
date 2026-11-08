"""Records of the cluster jobs this scheduler has submitted, persisted to a JSON file
so that limits and duplicate detection survive a scheduler restart."""
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple

PENDING = 'pending'
RUNNING = 'running'


@dataclass
class JobRecord:
    server: str          # server key
    service: str         # service key
    job: str             # job key
    ticket: int
    native_id: str       # SLURM/LSF job id
    submit_time: float
    spec_path: str
    state: str = PENDING

    @property
    def key(self) -> Tuple[str, int]:
        return (self.server, self.ticket)


class JobState:

    def __init__(self, path: str):
        self.path = path
        self.records: Dict[str, JobRecord] = {}   # by native_id

    def load(self):
        self.records = {}
        if not os.path.exists(self.path):
            return
        with open(self.path) as f:
            data = json.load(f)
        for r in data.get('jobs', []):
            rec = JobRecord(**r)
            self.records[rec.native_id] = rec

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self.path), prefix='.jobs.', suffix='.json')
        with os.fdopen(fd, 'w') as f:
            json.dump({'jobs': [asdict(r) for r in self.records.values()]}, f, indent=1)
        os.replace(tmp, self.path)

    def add(self, rec: JobRecord):
        self.records[rec.native_id] = rec

    def remove(self, native_id: str):
        self.records.pop(native_id, None)

    def all(self) -> List[JobRecord]:
        return list(self.records.values())

    def for_ticket(self, server: str, ticket: int) -> List[JobRecord]:
        return [r for r in self.records.values() if r.key == (server, ticket)]
