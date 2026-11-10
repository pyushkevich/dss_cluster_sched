#!/usr/bin/env python3
"""Example dss-cluster-sched job: trim the neck from a T1-weighted brain MRI.

This is a template for writing DSS service jobs in Python. It shows the
full life of a ticket on the compute node:

  1. connect to the DSS server with the scheduler's session
  2. download the ticket's ITK-SNAP workspace and find the input image
  3. do the work (here, one picsl_c3d command), reporting progress
  4. post log messages, with attachments: c3d's output and screenshots
  5. upload a result workspace and mark the ticket 'success'
  6. on any error, log it and mark the ticket 'failed'

Requirements in the job's environment: itksnap-dss >= 0.3.0 and picsl_c3d.
dss_cluster_sched is optional (see connect_to_dss below).

Example scheduler config:

  jobs:
    neck-trim:
      scheduler: {duration: "00:15:00", cpus: 1, memory: 4G}
      environment:
        venv: /shared/venvs/neck-trim
      command: >-
        python /shared/dss/neck_trim_job.py --ticket {ticket}
        --workdir {workdir} --head-height {head_height}
      defaults:
        workdir: /scratch/{user}/dss/{server_key}-{ticket}
        head_height: 180

  services:
    neck-trim:
      name: NeckTrim          # the service's name on the DSS server
      job: neck-trim

To try it by hand, outside the scheduler, claim a ticket first (e.g. with
`itksnap-wt -dssp-tickets-claim`) and pass --server; the script then uses
itksnap-dss's own login (stored in the system keyring).
"""
import argparse
import io
import os
import shutil
import sys
import tempfile
import traceback

# DSSClient touches the system keyring, which compute nodes usually lack
os.environ.setdefault('PYTHON_KEYRING_BACKEND', 'keyring.backends.null.Keyring')

from itksnap_dss import DSSClient                    # noqa: E402
from itksnap_dss.itksnap_ws import WorkspaceWrapper  # noqa: E402
from picsl_c3d import Convert3D                      # noqa: E402


def connect_to_dss(server=None) -> DSSClient:
    """A DSSClient logged in as the scheduler's DSS user.

    Under dss-cluster-sched, the job wrapper sets DSS_SERVER_URL and
    DSS_SESSION_FILE, and the session file holds a live DSS session id. Never
    call client.login(token) in a job: DSS tokens work only once.
    """
    if server is None and 'DSS_SESSION_FILE' in os.environ:
        try:
            # The supported way, if dss_cluster_sched is installed here
            from dss_cluster_sched.jobwrap import connect
            return connect()
        except ImportError:
            # Same thing by hand, for environments with only itksnap-dss
            client = DSSClient(os.environ['DSS_SERVER_URL'])
            with open(os.environ['DSS_SESSION_FILE']) as f:
                client.cli.cookies.set('webpy_session_id', f.read().strip())
            return client

    # Run by hand: use (or prompt for) itksnap-dss's own login
    client = DSSClient(server or os.environ['DSS_SERVER_URL'])
    client.login_if_needed()
    return client


def find_t1_layer(ws: WorkspaceWrapper, client: DSSClient, ticket: int) -> str:
    """Path of the T1 image: the layer tagged 'T1-MRI', or else the main image.
    Users tag layers in ITK-SNAP before submitting; the service's description
    should say which tags it expects."""
    layers = ws.find_layers_by_tag('T1-MRI')
    if len(layers) > 1:
        raise ValueError('More than one layer is tagged "T1-MRI"')
    if not layers:
        layers = [ws.find_layer_by_role('MainRole')]
        client.dssp_log(ticket, 'warning',
                        'No layer is tagged "T1-MRI"; using the main image')
    return ws.get_layer_actual_path(layers[0])


def screenshot(c3d: Convert3D, cmd: str, png: str):
    """Mid-sagittal slice of the image that `cmd` leaves on the c3d stack,
    saved as an 8-bit PNG with the face on the left and the top of the head up."""
    c3d.execute(f'-clear {cmd} -swapdim RAS -resample-mm 1x1x1mm -slice x 50% '
                f'-stretch 0.5% 99.5% 0 255 -clip 0 255 -type uchar -o {png}')


def process_ticket(client: DSSClient, ticket: int, workdir: str, head_height: float):
    input_dir = os.path.join(workdir, 'input')
    output_dir = os.path.join(workdir, 'output')
    os.makedirs(output_dir, exist_ok=True)

    # Progress is a number in [0, 1] shown in ITK-SNAP. Giving each step a
    # chunk (chunk_start, chunk_end) lets a step report its own 0..1 progress.
    client.dssp_log(ticket, 'info', 'Downloading the input workspace')
    client.dssp_download_ticket(ticket, input_dir)
    client.dssp_set_progress(ticket, 0.1)

    # The download holds one .itksnap workspace plus its (anonymized) images
    ws_files = [f for f in os.listdir(input_dir) if f.endswith('.itksnap')]
    if len(ws_files) != 1:
        raise ValueError('Ticket must contain exactly one ITK-SNAP workspace')
    ws = WorkspaceWrapper(os.path.join(input_dir, ws_files[0]))
    t1 = find_t1_layer(ws, client, ticket)

    # The actual work. Capture c3d's text output so it can be attached.
    c3d = Convert3D()
    out = io.StringIO()
    slab = os.path.join(output_dir, 'neck_slab.nii.gz')
    trimmed = os.path.join(output_dir, 't1_trimmed.nii.gz')
    c3d.execute(f'{t1} -info', out=out)
    client.dssp_set_progress(ticket, 0.0, chunk_start=0.1, chunk_end=0.6)
    c3d.execute(f'-clear {t1} -as T1 -neck-trim-head-height {head_height} -neck-trim '
                f'-o {slab} -trim 0vox -push T1 -reslice-identity -o {trimmed} -info',
                out=out)
    client.dssp_set_progress(ticket, 1.0, chunk_start=0.1, chunk_end=0.6)

    # Attachments go with the NEXT log message: attach first, then log
    c3d_log = os.path.join(output_dir, 'c3d_output.txt')
    with open(c3d_log, 'w') as f:
        f.write(out.getvalue())
    client.dssp_attach(ticket, 'Convert3D output', c3d_log, 'text/plain')
    client.dssp_log(ticket, 'info', f'Neck trimmed (head height {head_height} mm)')

    # Screenshots: the input with the trimmed-away part darkened, and the result
    shot_in = os.path.join(output_dir, 'trim_region.png')
    shot_out = os.path.join(output_dir, 'trimmed.png')
    screenshot(c3d, f'{t1} {slab} -scale 0.7 -shift 0.3 -multiply', shot_in)
    screenshot(c3d, trimmed, shot_out)
    client.dssp_attach(ticket, 'Input, region removed shown dark', shot_in, 'image/png')
    client.dssp_attach(ticket, 'Trimmed image', shot_out, 'image/png')
    client.dssp_log(ticket, 'info', 'Mid-sagittal screenshots attached')
    client.dssp_set_progress(ticket, 0.7)

    # The result is a workspace too: here the input workspace plus the trimmed
    # image as a new layer. dssp_upload_ticket exports the workspace with its
    # images and uploads them as the ticket's results.
    key = ws.add_layer('AnatomicalRole', trimmed)
    ws.set_layer_nickname(key, 'T1 neck-trimmed')
    result_ws = os.path.join(output_dir, 'result.itksnap')
    ws.save_workspace(result_ws)
    client.dssp_upload_ticket(ticket, result_ws, '_result')
    client.dssp_set_progress(ticket, 1.0)

    # Done: the user can now download the results in ITK-SNAP
    client.dssp_log(ticket, 'info', 'Uploaded the result workspace')
    client.dssp_set_status(ticket, 'success')


def main():
    p = argparse.ArgumentParser(description='DSS job: trim the neck from a T1 MRI')
    p.add_argument('--ticket', type=int, default=os.environ.get('DSS_TICKET'),
                   help='ticket id (default: $DSS_TICKET)')
    p.add_argument('--workdir', help='scratch directory (default: a new temp dir)')
    p.add_argument('--head-height', type=float, default=180, help='mm (c3d default 180)')
    p.add_argument('--keep-workdir', action='store_true')
    p.add_argument('--server', help='DSS server URL, only when run by hand')
    a = p.parse_args()
    if a.ticket is None:
        p.error('--ticket is required')
    ticket = int(a.ticket)

    client = connect_to_dss(a.server)
    workdir = a.workdir or tempfile.mkdtemp(prefix=f'dss-necktrim-{ticket}-')
    os.makedirs(workdir, exist_ok=True)
    try:
        process_ticket(client, ticket, workdir, a.head_height)
        return 0
    except Exception as e:
        # Report a readable error to the user and close the ticket. The
        # traceback goes to the job's output file on the cluster. (If the
        # script died without doing this, dss-cluster-sched's job wrapper would
        # mark the ticket failed and attach the end of the output instead.)
        traceback.print_exc()
        client.dssp_log(ticket, 'error', f'Neck trimming failed: {e}')
        client.dssp_set_status(ticket, 'failed')
        return 1
    finally:
        if a.keep_workdir:
            print(f'Keeping {workdir}')
        else:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())
