#!/bin/bash
# Starts munge, slurmctld, slurmd and the DSS server, bootstraps the DSS test
# data (users, provider, dummy services, scheduler config), then runs:
#   scheduler      dss-cluster-sched run on the generated config (default)
#   test           the end-to-end tests (docker/test_e2e.py)
#   anything else  that command, as user dss (e.g. bash)
set -e

HOST=$(hostname)
# Describe this node with its real CPU/memory (slurmd drains a node whose
# configured CPUs exceed what it has), minus the parts we don't want
NODE=$(slurmd -C | head -1 | sed -e 's/ UpTime=.*//')
sed -e "s|@HOST@|$HOST|" -e "s|@NODE@|$NODE|" /opt/src/docker/slurm.conf.in > /etc/slurm/slurm.conf
cp /opt/src/docker/cgroup.conf /etc/slurm/cgroup.conf

if [ ! -f /etc/munge/munge.key ]; then
  dd if=/dev/urandom bs=1 count=1024 of=/etc/munge/munge.key 2>/dev/null
fi
chown munge:munge /etc/munge/munge.key /run/munge
chmod 400 /etc/munge/munge.key
runuser -u munge -- /usr/sbin/munged

/usr/sbin/slurmctld
/usr/sbin/slurmd

# DSS server, as user dss, on port 8080
DSSHOME=/home/dss/dss-server
mkdir -p $DSSHOME
chown -R dss:dss $DSSHOME
runuser -u dss -- bash -c "cd $DSSHOME && exec /opt/dss/bin/itksnap-dss-server --server \
    --port 8080 --cookie-domain '' --sqlite-path $DSSHOME/dss.sqlite3 \
    --datastore-root $DSSHOME/datastore >> $DSSHOME/server.log 2>&1" &

# Wait for SLURM to accept jobs
for i in $(seq 1 30); do
  sinfo -h -o '%T' 2>/dev/null | grep -qE 'idle|mixed|alloc' && break
  sleep 1
done
sinfo

runuser -u dss -- /opt/dss/bin/python /opt/src/docker/bootstrap.py

case "$1" in
  scheduler)
    exec runuser -u dss -- /opt/dss/bin/dss-cluster-sched run /home/dss/e2e/config.yaml ;;
  test)
    shift
    cd /opt/src
    exec runuser -u dss -- /opt/dss/bin/python -m pytest -v -p no:cacheprovider docker/test_e2e.py "$@" ;;
  *)
    exec runuser -u dss -- "$@" ;;
esac
