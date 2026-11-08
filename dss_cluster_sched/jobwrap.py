"""Runs on the compute node: `python -m dss_cluster_sched.jobwrap <spec.json>`.

1. Claims the ticket for good (POST /api/pro/tickets/<id>/claim). If the ticket
   is no longer available to this provider instance (its temporary claim
   lapsed and someone else took it, or it is already being worked on), exits 0
   without running anything.
2. Runs the job's environment setup and command in bash, with the template
   variables exported as DSS_<NAME>.
3. If the command fails (or the job is terminated) while the ticket is still
   claimed, attaches the tail of the output to the ticket log and marks the
   ticket failed. A ticket the command already marked success/failed is left alone.

Python job scripts can call connect() to get a DSSClient using the
scheduler's session.
"""
import collections
import json
import os
import shlex
import signal
import subprocess
import sys
import tempfile

os.environ.setdefault('PYTHON_KEYRING_BACKEND', 'keyring.backends.null.Keyring')

from .session import connect  # noqa: E402,F401  (re-exported for job scripts)

TAIL_LINES = 200


def build_script(spec) -> str:
    """The bash script that sets up the environment and runs the command. Order:
    exported variables and env, setup snippet, module loads, venv activation."""
    lines = ['#!/bin/bash']
    for k, v in spec['variables'].items():
        lines.append('export DSS_%s=%s' % (k.upper(), shlex.quote(str(v))))
    for k, v in spec['env'].items():
        lines.append('export %s=%s' % (k, shlex.quote(v)))
    if spec['setup']:
        lines.append(spec['setup'])
    for m in spec['modules']:
        lines.append('module load %s || exit 1' % m)
    if spec['venv']:
        lines.append('source %s/bin/activate || exit 1' % shlex.quote(spec['venv']))
    lines.append('exec %s' % spec['command'])
    return '\n'.join(lines) + '\n'


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print('usage: python -m dss_cluster_sched.jobwrap <spec.json>', file=sys.stderr)
        return 2
    with open(argv[0]) as f:
        spec = json.load(f)

    ticket = int(spec['ticket'])
    client = connect(spec['server_url'], spec['session_file'])

    # 1. Claim the ticket for good, or bow out
    if not client.dssp_renew_claim(ticket, spec['provider'], spec['provider_code']):
        print('dss-jobwrap: ticket %d is no longer available to %s/%s; nothing to do'
              % (ticket, spec['provider'], spec['provider_code']), flush=True)
        return 0
    print('dss-jobwrap: claimed ticket %d on %s' % (ticket, spec['server_url']), flush=True)
    client.dssp_log(ticket, 'info', 'Processing started on %s' % os.uname().nodename)

    # 2. Run the command
    if spec.get('directory'):
        os.makedirs(spec['directory'], exist_ok=True)
        os.chdir(spec['directory'])
    with tempfile.NamedTemporaryFile('w', suffix='.sh', delete=False) as f:
        f.write(build_script(spec))
        script = f.name

    tail = collections.deque(maxlen=TAIL_LINES)
    proc = subprocess.Popen(['/bin/bash', script], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, errors='replace')
    terminated = []

    def on_signal(signum, frame):
        terminated.append(signum)
        proc.terminate()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        tail.append(line)
    rc = proc.wait()
    os.unlink(script)

    if rc == 0 and not terminated:
        print('dss-jobwrap: command finished', flush=True)
        return 0

    # 3. Failure: report on the ticket, unless the command already closed it
    reason = ('Job terminated by signal %d' % terminated[0]) if terminated \
        else ('Job command failed with exit code %d' % rc)
    print('dss-jobwrap: ' + reason, flush=True)
    try:
        status = client.get_('api/pro/tickets/%d/status' % ticket).text.strip()
        if status == 'claimed':
            with tempfile.NamedTemporaryFile('w', suffix='.txt', prefix='output_',
                                             delete=False) as f:
                f.writelines(tail)
                tail_file = f.name
            try:
                client.dssp_attach(ticket, 'Job output (last %d lines)' % TAIL_LINES,
                                   tail_file, 'text/plain')
            finally:
                os.unlink(tail_file)
            client.dssp_log(ticket, 'error', reason)
            client.dssp_set_status(ticket, 'failed')
        else:
            print('dss-jobwrap: ticket status is already %r; not marking it failed' % status)
    except Exception as e:
        print('dss-jobwrap: could not report the failure to the server: %s' % e, flush=True)
    return rc if rc != 0 else 1


if __name__ == '__main__':
    sys.exit(main())
