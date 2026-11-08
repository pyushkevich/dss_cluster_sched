"""DSS login sessions.

A DSS login token works only once: /api/login replaces it with a new random
token. What lasts is the session it creates (the server keeps sessions for a
year and ignores expiry). So the scheduler logs in once, keeps the session id
in a file readable only by the user, and passes that file to its jobs, which
reuse the same session.
"""
import contextlib
import io
import os

os.environ.setdefault('PYTHON_KEYRING_BACKEND', 'keyring.backends.null.Keyring')

COOKIE = 'webpy_session_id'


class LoginError(Exception):
    pass


def read_session(path):
    try:
        with open(path) as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


def write_session(path, session_id):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path + '.tmp', os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        f.write(session_id + '\n')
    os.replace(path + '.tmp', path)


def use_session(client, session_id):
    client.cli.cookies.clear()
    client.cli.cookies.set(COOKIE, session_id)


def session_valid(client) -> bool:
    """True if the client's session is logged in on the server."""
    r = client.cli.get('%s/api/login' % client.server)
    return r.status_code == 200 and r.text.startswith('logged in as')


def token_login(client, token) -> str:
    """Log in with a one-time token; returns the new session id."""
    with contextlib.redirect_stdout(io.StringIO()):   # DSSClient.login prints a keychain note
        r = client.login(token)
    sid = r.cookies.get(COOKIE)
    if not sid:
        raise LoginError('Login to %s did not return a session' % client.server)
    return sid


def connect(server_url=None, session_file=None):
    """A DSSClient using the session in session_file. For job scripts: by default
    uses the DSS_SERVER_URL and DSS_SESSION_FILE variables set by jobwrap."""
    from itksnap_dss import DSSClient
    server_url = server_url or os.environ['DSS_SERVER_URL']
    session_file = session_file or os.environ['DSS_SESSION_FILE']
    sid = read_session(session_file)
    if not sid:
        raise LoginError('No DSS session in %s' % session_file)
    client = DSSClient(server_url)
    use_session(client, sid)
    return client
