"""Loading and validation of the scheduler's YAML configuration."""
import datetime
import getpass
import os
import re
import socket
import string
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import yaml

KEY_RE = re.compile(r'^[A-Za-z0-9_-]+$')
GITHASH_RE = re.compile(r'^[0-9a-f]{40}$')

# Template variables supplied by the scheduler for every job
BUILTIN_VARS = (
    'server_url', 'server_key', 'ticket', 'service_key', 'service_name',
    'service_githash', 'job_key', 'provider', 'provider_code', 'session_file',
    'instance', 'user',
)

SCHEDULER_KEYS = {
    'queue', 'account', 'project', 'reservation', 'duration', 'nodes',
    'processes', 'processes_per_node', 'cpus', 'gpus', 'memory', 'exclusive',
    'custom', 'directory',
}
ENVIRONMENT_KEYS = {'setup', 'modules', 'venv', 'env'}


class ConfigError(Exception):
    pass


@dataclass
class Limits:
    max_jobs: Optional[int] = None    # queued + running
    max_queued: Optional[int] = None  # queued (pending) only


@dataclass
class Server:
    key: str
    url: str
    provider: str
    session_file: str               # holds the DSS session id (shared with the jobs)
    token_file: Optional[str] = None  # one-time login token, used only to create the session
    limits: Limits = field(default_factory=Limits)


@dataclass
class JobType:
    key: str
    command: str
    scheduler: Dict = field(default_factory=dict)
    environment: Dict = field(default_factory=dict)
    defaults: Dict[str, str] = field(default_factory=dict)
    limits: Limits = field(default_factory=Limits)


@dataclass
class Service:
    key: str
    job: str
    name: Optional[str] = None
    githash: Optional[str] = None
    servers: List[str] = field(default_factory=list)
    params: Dict[str, str] = field(default_factory=dict)
    limits: Limits = field(default_factory=Limits)


@dataclass
class Config:
    instance: str
    executor: str
    poll_interval: float
    claim_timeout: int
    state_dir: str
    python: str
    service_refresh: float
    limits: Limits
    servers: Dict[str, Server]
    jobs: Dict[str, JobType]
    services: Dict[str, Service]


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------

def template_fields(template: str) -> List[str]:
    """Names referenced by a template, e.g. 'a {x} {y}' -> ['x', 'y']."""
    names = []
    for _, fname, spec, conv in string.Formatter().parse(template):
        if fname is None:
            continue
        if not KEY_RE.match(fname) or spec or conv:
            raise ConfigError("Unsupported template field '{%s}' in %r: only plain {name} "
                              "fields are allowed (write '{{' and '}}' for literal braces)"
                              % (fname, template))
        names.append(fname)
    return names


def render(template: str, variables: Dict[str, object]) -> str:
    """Fill {name} fields of template from variables. Variable values may themselves
    be templates referring to other variables; cycles and unknown names are errors."""
    resolved: Dict[str, str] = {}

    def resolve(name, stack):
        if name in resolved:
            return resolved[name]
        if name not in variables:
            raise ConfigError("Unknown template variable '{%s}'" % name)
        if name in stack:
            raise ConfigError("Template variables refer to each other in a cycle: %s"
                              % ' -> '.join(stack + [name]))
        value = str(variables[name])
        resolved[name] = expand(value, stack + [name])
        return resolved[name]

    def expand(text, stack):
        return string.Formatter().vformat(
            text, (), {n: resolve(n, stack) for n in template_fields(text)})

    return expand(template, [])


def job_variables(job: JobType, service: Service, builtins: Dict[str, object]) -> Dict[str, object]:
    """Variables available to a job's templates: built-ins, then job defaults,
    then service params (later ones override earlier ones)."""
    v = dict(builtins)
    v.update(job.defaults)
    v.update(service.params)
    return v


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_duration(value) -> datetime.timedelta:
    """Walltime: minutes (int), or 'MM:SS', 'HH:MM:SS', 'D-HH:MM:SS'."""
    if isinstance(value, (int, float)):
        return datetime.timedelta(minutes=value)
    m = re.match(r'^(?:(\d+)-)?(\d+)(?::(\d+))?(?::(\d+))?$', str(value).strip())
    if not m:
        raise ConfigError("Invalid duration %r (use minutes, 'HH:MM:SS' or 'D-HH:MM:SS')" % value)
    days, a, b, c = m.groups()
    parts = [int(x) for x in (a, b, c) if x is not None]
    if len(parts) == 3:
        h, mi, s = parts
    elif len(parts) == 2:
        h, mi, s = (0, parts[0], parts[1]) if days is None else (parts[0], parts[1], 0)
    else:
        h, mi, s = (0, parts[0], 0) if days is None else (parts[0], 0, 0)
    return datetime.timedelta(days=int(days or 0), hours=h, minutes=mi, seconds=s)


def parse_memory(value) -> int:
    """Memory in bytes from e.g. 16G, 512M, 1.5T, or a plain number of bytes."""
    if isinstance(value, int):
        return value
    m = re.match(r'^\s*([\d.]+)\s*([KMGT]?)i?B?\s*$', str(value), re.IGNORECASE)
    if not m:
        raise ConfigError("Invalid memory %r (use e.g. 16G or 512M)" % value)
    mult = {'': 1, 'K': 2**10, 'M': 2**20, 'G': 2**30, 'T': 2**40}[m.group(2).upper()]
    return int(float(m.group(1)) * mult)


def _check_keys(where, d, allowed):
    unknown = set(d) - set(allowed)
    if unknown:
        raise ConfigError("%s: unknown key(s) %s (allowed: %s)"
                          % (where, ', '.join(sorted(unknown)), ', '.join(sorted(allowed))))


def _dict(where, value):
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError("%s must be a mapping" % where)
    return value


def _positive_int(where, value, allow_zero=False):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < (0 if allow_zero else 1):
        raise ConfigError("%s must be a %s integer" % (where, 'non-negative' if allow_zero else 'positive'))
    return value


def _limits(where, d) -> Limits:
    d = _dict(where, d)
    _check_keys(where, d, {'max_jobs', 'max_queued'})
    return Limits(max_jobs=_positive_int(where + '.max_jobs', d.get('max_jobs'), True),
                  max_queued=_positive_int(where + '.max_queued', d.get('max_queued'), True))


def _key(where, k):
    if not isinstance(k, str) or not KEY_RE.match(k):
        raise ConfigError("%s: key %r must consist of letters, digits, '_' and '-'" % (where, k))
    return k


def _str_dict(where, d):
    d = _dict(where, d)
    out = {}
    for k, v in d.items():
        _key(where, k)
        if isinstance(v, (dict, list)) or v is None:
            raise ConfigError("%s.%s must be a string or number" % (where, k))
        out[k] = str(v)
    return out


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_config(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return parse_config(raw, base_dir=os.path.dirname(os.path.abspath(path)))


def parse_config(raw, base_dir='.') -> Config:
    raw = _dict('config', raw)
    _check_keys('config', raw, {'instance', 'executor', 'poll_interval', 'claim_timeout',
                                'state_dir', 'python', 'service_refresh', 'limits',
                                'servers', 'jobs', 'services'})

    def path(p):
        return os.path.normpath(os.path.join(base_dir, os.path.expanduser(str(p))))

    instance = _key('instance', str(raw.get('instance') or socket.gethostname().split('.')[0]))
    executor = str(raw.get('executor', 'slurm'))
    claim_timeout = _positive_int('claim_timeout', raw.get('claim_timeout', 120))

    servers = {}
    for k, s in _dict('servers', raw.get('servers')).items():
        where = 'servers.%s' % _key('servers', k)
        s = _dict(where, s)
        _check_keys(where, s, {'url', 'token_file', 'session_file', 'provider', 'limits'})
        for req in ('url', 'provider'):
            if not s.get(req):
                raise ConfigError("%s.%s is required" % (where, req))
        servers[k] = Server(
            key=k, url=str(s['url']).rstrip('/'), provider=str(s['provider']),
            session_file=path(s['session_file']) if s.get('session_file') else None,
            token_file=path(s['token_file']) if s.get('token_file') else None,
            limits=_limits(where + '.limits', s.get('limits')))
    if not servers:
        raise ConfigError("At least one server must be configured under 'servers'")

    jobs = {}
    for k, j in _dict('jobs', raw.get('jobs')).items():
        where = 'jobs.%s' % _key('jobs', k)
        j = _dict(where, j)
        _check_keys(where, j, {'command', 'scheduler', 'environment', 'defaults', 'limits'})
        if not j.get('command'):
            raise ConfigError("%s.command is required" % where)
        sched = _dict(where + '.scheduler', j.get('scheduler'))
        _check_keys(where + '.scheduler', sched, SCHEDULER_KEYS)
        if 'duration' in sched:
            parse_duration(sched['duration'])
        if 'memory' in sched:
            parse_memory(sched['memory'])
        for ik in ('nodes', 'processes', 'processes_per_node', 'cpus', 'gpus'):
            _positive_int('%s.scheduler.%s' % (where, ik), sched.get(ik))
        custom = _dict(where + '.scheduler.custom', sched.get('custom'))
        for ck in custom:
            if '.' not in str(ck):
                raise ConfigError("%s.scheduler.custom: key %r must be prefixed by the executor "
                                  "name, e.g. 'slurm.constraint' or 'lsf.R'" % (where, ck))
        env = _dict(where + '.environment', j.get('environment'))
        _check_keys(where + '.environment', env, ENVIRONMENT_KEYS)
        modules = env.get('modules') or []
        if isinstance(modules, str):
            modules = [modules]
        env = {'setup': str(env.get('setup') or ''), 'modules': [str(m) for m in modules],
               'venv': str(env['venv']) if env.get('venv') else None,
               'env': _str_dict(where + '.environment.env', env.get('env'))}
        jobs[k] = JobType(key=k, command=str(j['command']).strip(), scheduler=sched,
                          environment=env, defaults=_str_dict(where + '.defaults', j.get('defaults')),
                          limits=_limits(where + '.limits', j.get('limits')))
    if not jobs:
        raise ConfigError("At least one job must be configured under 'jobs'")

    services = {}
    for k, s in _dict('services', raw.get('services')).items():
        where = 'services.%s' % _key('services', k)
        s = _dict(where, s)
        _check_keys(where, s, {'name', 'githash', 'job', 'servers', 'params', 'limits'})
        if bool(s.get('name')) == bool(s.get('githash')):
            raise ConfigError("%s: exactly one of 'name' or 'githash' is required" % where)
        if s.get('githash') and not GITHASH_RE.match(str(s['githash'])):
            raise ConfigError("%s.githash must be a 40-character hex git hash" % where)
        if s.get('job') not in jobs:
            raise ConfigError("%s.job %r is not one of the configured jobs (%s)"
                              % (where, s.get('job'), ', '.join(jobs)))
        svc_servers = s.get('servers') or list(servers)
        if isinstance(svc_servers, str):
            svc_servers = [svc_servers]
        for sk in svc_servers:
            if sk not in servers:
                raise ConfigError("%s.servers: %r is not one of the configured servers" % (where, sk))
        services[k] = Service(key=k, job=s['job'], name=s.get('name'), githash=s.get('githash'),
                              servers=list(svc_servers),
                              params=_str_dict(where + '.params', s.get('params')),
                              limits=_limits(where + '.limits', s.get('limits')))
    if not services:
        raise ConfigError("At least one service must be configured under 'services'")

    state_dir = path(raw.get('state_dir', '~/.dss_cluster_sched'))
    for srv in servers.values():
        if srv.session_file is None:
            srv.session_file = os.path.join(state_dir, 'sessions', srv.key + '.session')

    cfg = Config(
        instance=instance, executor=executor,
        poll_interval=float(raw.get('poll_interval', 30)),
        claim_timeout=claim_timeout,
        state_dir=state_dir,
        python=str(raw.get('python') or sys.executable),
        service_refresh=float(raw.get('service_refresh', 300)),
        limits=_limits('limits', raw.get('limits')),
        servers=servers, jobs=jobs, services=services)

    # Check that every template used by each service's job renders
    for svc in services.values():
        job = jobs[svc.job]
        dummy = {b: 'X' for b in BUILTIN_VARS}
        variables = job_variables(job, svc, dummy)
        for what, tmpl in _job_templates(job):
            try:
                render(tmpl, variables)
            except ConfigError as e:
                raise ConfigError("services.%s (job %s), %s: %s" % (svc.key, job.key, what, e))
    return cfg


def _job_templates(job: JobType):
    """All templated strings of a job, as (description, template) pairs."""
    yield 'command', job.command
    if job.environment['setup']:
        yield 'environment.setup', job.environment['setup']
    for m in job.environment['modules']:
        yield 'environment.modules', m
    if job.environment['venv']:
        yield 'environment.venv', job.environment['venv']
    for k, v in job.environment['env'].items():
        yield 'environment.env.%s' % k, v
    if job.scheduler.get('directory'):
        yield 'scheduler.directory', str(job.scheduler['directory'])


def current_user():
    try:
        return getpass.getuser()
    except Exception:
        return str(os.getuid())
