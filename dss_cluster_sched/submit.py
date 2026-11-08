"""Turning a claimed ticket into a cluster job: template variables, the job spec
file read by jobwrap on the compute node, and the psij JobSpec."""
import json
import os
import time
from typing import Dict

from .config import (Config, JobType, Server, Service, _job_templates, current_user,
                     job_variables, parse_duration, parse_memory, render)


# Executor-specific option that sets the job name (see psij_job_spec)
JOB_NAME_ATTRIBUTE = {'slurm': 'slurm.job-name', 'lsf': 'lsf.J'}


def builtin_variables(cfg: Config, server: Server, service: Service, githash: str,
                      ticket: int) -> Dict[str, object]:
    return {
        'server_url': server.url, 'server_key': server.key, 'ticket': ticket,
        'service_key': service.key, 'service_name': service.name or '',
        'service_githash': githash, 'job_key': service.job,
        'provider': server.provider, 'provider_code': cfg.instance,
        'session_file': server.session_file, 'instance': cfg.instance, 'user': current_user(),
    }


def job_name(cfg: Config, server: Server, ticket: int) -> str:
    return 'dss.%s.%s.%d' % (cfg.instance, server.key, ticket)


def render_job(cfg: Config, server: Server, service: Service, githash: str, ticket: int) -> Dict:
    """Everything jobwrap needs to run the job, with templates filled in."""
    job: JobType = cfg.jobs[service.job]
    variables = job_variables(job, service, builtin_variables(cfg, server, service, githash, ticket))
    resolved = {k: render(str(v), variables) for k, v in variables.items()}
    rendered = {what: render(t, variables) for what, t in _job_templates(job)}
    env = job.environment
    return {
        'server_url': server.url,
        'session_file': server.session_file,
        'provider': server.provider,
        'provider_code': cfg.instance,
        'ticket': ticket,
        'command': rendered['command'],
        'setup': render(env['setup'], variables) if env['setup'] else '',
        'modules': [render(m, variables) for m in env['modules']],
        'venv': render(env['venv'], variables) if env['venv'] else None,
        'env': {k: render(v, variables) for k, v in env['env'].items()},
        'directory': rendered.get('scheduler.directory'),
        'variables': resolved,
    }


def write_spec_file(cfg: Config, server: Server, ticket: int, spec: Dict) -> str:
    d = os.path.join(cfg.state_dir, 'jobs')
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, '%s-%08d-%d.json' % (server.key, ticket, int(time.time() * 1000)))
    with open(path, 'w') as f:
        json.dump(spec, f, indent=1)
    return path


def psij_job_spec(cfg: Config, job: JobType, name: str, spec_path: str):
    """A psij JobSpec running jobwrap on the spec file, with the job's scheduler options.
    The job starts in the log directory; jobwrap creates and enters the job's own
    'directory' (if configured) on the compute node."""
    import psij

    s = job.scheduler
    custom = {str(k): str(v) for k, v in (s.get('custom') or {}).items()}
    # psij 0.9.11's batch templates read the job name from an attribute that
    # does not exist, so JobSpec.name is ignored; pass it as a custom attribute
    if name and cfg.executor in JOB_NAME_ATTRIBUTE:
        custom.setdefault(JOB_NAME_ATTRIBUTE[cfg.executor], name)
    attrs = psij.JobAttributes(
        duration=parse_duration(s['duration']) if 'duration' in s else parse_duration('01:00:00'),
        queue_name=s.get('queue'), account=s.get('account'), project_name=s.get('project'),
        reservation_id=s.get('reservation'), custom_attributes=custom or None)

    res_args = {}
    if 'nodes' in s:
        res_args['node_count'] = s['nodes']
    if 'processes' in s:
        res_args['process_count'] = s['processes']
    if 'processes_per_node' in s:
        res_args['processes_per_node'] = s['processes_per_node']
    if 'cpus' in s:
        res_args['cpu_cores_per_process'] = s['cpus']
    if 'gpus' in s:
        res_args['gpu_cores_per_process'] = s['gpus']
    if 'memory' in s:
        res_args['memory'] = parse_memory(s['memory'])
    if s.get('exclusive'):
        res_args['exclusive_node_use'] = True
    resources = psij.ResourceSpecV1(**res_args) if res_args else None

    log_dir = os.path.join(cfg.state_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(spec_path))[0]
    return psij.JobSpec(
        executable=cfg.python,
        arguments=['-m', 'dss_cluster_sched.jobwrap', spec_path],
        name=name,
        directory=log_dir,
        environment={'PYTHON_KEYRING_BACKEND': 'keyring.backends.null.Keyring'},
        stdout_path=os.path.join(log_dir, base + '.out'),
        stderr_path=os.path.join(log_dir, base + '.err'),
        attributes=attrs,
        resources=resources,
        launcher='single')
