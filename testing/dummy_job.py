"""A dummy DSS job for testing dss_cluster_sched: logs its parameters on the
ticket, reports progress while sleeping, then marks the ticket success (or
exits non-zero with --fail, to exercise jobwrap's failure reporting)."""
import argparse
import os
import sys
import time

from dss_cluster_sched.jobwrap import connect


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--ticket', type=int, required=True)
    p.add_argument('--atlas', default='none')
    p.add_argument('--duration', type=float, default=1.0)
    p.add_argument('--fail', default='no', help="'yes' to fail instead of succeeding")
    a = p.parse_args()

    c = connect()
    c.dssp_log(a.ticket, 'info', 'dummy job: atlas=%s duration=%s service=%s host=%s'
               % (a.atlas, a.duration, os.environ.get('DSS_SERVICE_KEY'), os.uname().nodename))
    steps = 4
    for i in range(steps):
        time.sleep(a.duration / steps)
        c.dssp_set_progress(a.ticket, (i + 1) / steps)
    if a.fail == 'yes':
        print('dummy job: failing on purpose')
        return 3
    c.dssp_set_status(a.ticket, 'success')
    return 0


if __name__ == '__main__':
    sys.exit(main())
