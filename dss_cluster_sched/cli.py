"""Command line: dss-cluster-sched {run,validate,status} CONFIG"""
import argparse
import logging
import os
import signal
import sys
import time

from .config import ConfigError, load_config


def cmd_validate(cfg, args):
    print('Configuration OK: instance %s, executor %s, %d server(s), %d job(s), %d service(s)'
          % (cfg.instance, cfg.executor, len(cfg.servers), len(cfg.jobs), len(cfg.services)))
    from .session import read_session
    for s in cfg.servers.values():
        if not read_session(s.session_file) and not (s.token_file and os.path.exists(s.token_file)):
            print('warning: server %s has no session yet (%s) and no token_file; run '
                  '`dss-cluster-sched login %s %s`' % (s.key, s.session_file, args.config, s.key))
    return 0


def cmd_login(cfg, args):
    """Turn a one-time token from <server>/token into a saved session."""
    import getpass
    from itksnap_dss import DSSClient
    from .session import read_session, session_valid, token_login, use_session, write_session
    keys = args.servers or list(cfg.servers)
    for k in keys:
        if k not in cfg.servers:
            print('dss-cluster-sched: unknown server %r (configured: %s)' % (k, ', '.join(cfg.servers)),
                  file=sys.stderr)
            return 2
    rc = 0
    for k in keys:
        srv = cfg.servers[k]
        client = DSSClient(srv.url)
        sid = read_session(srv.session_file)
        if sid and not args.force:
            use_session(client, sid)
            if session_valid(client):
                print('%s: already logged in (session in %s); use --force to replace it'
                      % (k, srv.session_file))
                continue
        print('%s: visit %s/token while logged in as the provider user, and paste the token.'
              % (k, srv.url))
        token = getpass.getpass('Token for %s: ' % k).strip()
        try:
            sid = token_login(client, token)
        except Exception as e:
            print('%s: login failed: %s' % (k, e), file=sys.stderr)
            rc = 1
            continue
        write_session(srv.session_file, sid)
        print('%s: logged in; session saved to %s' % (k, srv.session_file))
    return rc


def cmd_status(cfg, args):
    from .limits import LimitTracker
    from .state import JobState
    st = JobState(os.path.join(cfg.state_dir, 'jobs.json'))
    st.load()
    recs = sorted(st.all(), key=lambda r: r.submit_time)
    print('%-12s %-10s %-10s %8s %-20s %-12s %8s' % ('JOBID', 'STATE', 'SERVER', 'TICKET',
                                                   'SERVICE', 'JOB', 'AGE'))
    now = time.time()
    for r in recs:
        print('%-12s %-10s %-10s %8d %-20s %-12s %7.0fs' % (r.native_id, r.state, r.server, r.ticket,
                                                         r.service, r.job, now - r.submit_time))
    print(LimitTracker(cfg, recs).summary() + ' (as of the last scheduler update)')
    return 0


def cmd_run(cfg, args):
    from .backend import make_backend
    from .scheduler import Scheduler
    sched = Scheduler(cfg, make_backend(cfg))

    def stop(signum, frame):
        logging.getLogger(__name__).info('Signal %d received, stopping after this step', signum)
        sched.stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logging.getLogger(__name__).info(
        'Starting: instance %s, executor %s, servers %s, services %s', cfg.instance,
        cfg.executor, ', '.join(cfg.servers), ', '.join(cfg.services))
    sched.run(once=args.once, dry_run=args.dry_run)
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog='dss-cluster-sched',
                                description='Schedule ITK-SNAP DSS tickets as SLURM/LSF jobs')
    p.add_argument('-v', '--verbose', action='store_true', help='debug logging')
    sub = p.add_subparsers(dest='command', required=True)
    r = sub.add_parser('run', help='run the scheduler')
    r.add_argument('config')
    r.add_argument('--once', action='store_true', help='run a single cycle and exit')
    r.add_argument('--dry-run', action='store_true',
                   help='show what would be claimed, without claiming or submitting anything')
    v = sub.add_parser('validate', help='check the configuration file')
    v.add_argument('config')
    s = sub.add_parser('status', help='list the jobs the scheduler is tracking')
    s.add_argument('config')
    lg = sub.add_parser('login', help='log in to the servers with a one-time token')
    lg.add_argument('config')
    lg.add_argument('servers', nargs='*', help='server keys (default: all)')
    lg.add_argument('--force', action='store_true', help='replace a session that still works')
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    if not args.verbose:
        logging.getLogger('httpx').setLevel(logging.WARNING)
        logging.getLogger('psij').setLevel(logging.WARNING)

    try:
        cfg = load_config(args.config)
    except (ConfigError, OSError) as e:
        print('dss-cluster-sched: %s' % e, file=sys.stderr)
        return 2
    commands = {'run': cmd_run, 'validate': cmd_validate, 'status': cmd_status, 'login': cmd_login}
    return commands[args.command](cfg, args)


if __name__ == '__main__':
    sys.exit(main())
