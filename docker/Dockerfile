# Single-node SLURM cluster + local ITK-SNAP DSS server, for testing
# dss-cluster-sched end to end. Build from the repo root:
#   docker compose build
FROM debian:trixie

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      slurm-wlm munge python3 python3-venv git ca-certificates procps sudo \
    && rm -rf /var/lib/apt/lists/*

# The cluster user that runs the DSS server, the scheduler and the jobs. It may
# take the debug partition down/up (to test jobs stuck in the queue).
RUN useradd -m -s /bin/bash dss \
    && echo 'dss ALL=(root) NOPASSWD: /usr/bin/scontrol' > /etc/sudoers.d/dss \
    && mkdir -p /var/spool/slurmctld /var/spool/slurmd /var/log/slurm /run/munge \
    && chown slurm:slurm /var/spool/slurmctld /var/log/slurm

# Server and client from GitHub (public repos; both include temporary-claim
# support). Override the refs with --build-arg to test other branches.
ARG ALFABIS_SERVER_REF=master
ARG ITKSNAP_DSS_REF=main
RUN python3 -m venv /opt/dss \
    && /opt/dss/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dss/bin/pip install --no-cache-dir \
         "itksnap-dss-server @ git+https://github.com/pyushkevich/alfabis_server@${ALFABIS_SERVER_REF}" \
         "itksnap-dss @ git+https://github.com/pyushkevich/itksnap-dss-python@${ITKSNAP_DSS_REF}" \
         pytest

# This repo (editable, so a bind mount of the source over /opt/src picks up edits)
COPY . /opt/src
RUN /opt/dss/bin/pip install --no-cache-dir --no-deps -e /opt/src \
    && /opt/dss/bin/pip install --no-cache-dir psij-python==0.9.11 pyyaml

ENV PATH=/opt/dss/bin:$PATH
EXPOSE 8080
ENTRYPOINT ["/opt/src/docker/entrypoint.sh"]
CMD ["scheduler"]
