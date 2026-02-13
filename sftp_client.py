"""SFTP client — connect, recursive walk, read file."""

import os
import stat

import paramiko
from dotenv import load_dotenv


def connect(env_path=None):
    """Connect to SFTP server using credentials from .env. Returns SFTPClient."""
    if env_path is None:
        env_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(env_path)

    host = os.environ["SFTP_HOST"]
    port = int(os.environ.get("SFTP_PORT", 22))
    user = os.environ["SFTP_USER"]
    password = os.environ["SFTP_PASSWORD"]

    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp


def walk_remote(sftp, root, extensions=None, max_depth=None):
    """Recursively walk a remote directory, returning file entries.

    Returns list of dicts: {path, stat} for files matching extensions.
    If extensions is None, returns all files.
    max_depth limits recursion depth (None = unlimited).
    """
    if extensions is None:
        extensions = {".sas"}
    else:
        extensions = {e.lower() for e in extensions}

    results = []
    _walk_recursive(sftp, root, extensions, results, 0, max_depth)
    return results


def _walk_recursive(sftp, path, extensions, results, depth, max_depth):
    """Recursive helper for walk_remote."""
    if max_depth is not None and depth > max_depth:
        return
    try:
        entries = sftp.listdir_attr(path)
    except IOError:
        return

    for entry in entries:
        # Skip hidden directories to avoid crawling .cache, .vscode-server, etc.
        if entry.filename.startswith("."):
            continue
        full_path = f"{path}/{entry.filename}" if not path.endswith("/") else f"{path}{entry.filename}"
        if stat.S_ISDIR(entry.st_mode):
            _walk_recursive(sftp, full_path, extensions, results, depth + 1, max_depth)
        elif stat.S_ISREG(entry.st_mode):
            _, ext = os.path.splitext(entry.filename)
            if ext.lower() in extensions:
                results.append({
                    "path": full_path,
                    "stat": entry,
                })


def read_file(sftp, path):
    """Read a remote file and return contents as string."""
    with sftp.open(path, "r") as f:
        return f.read().decode("utf-8", errors="replace")


def close(sftp):
    """Close the SFTP connection and underlying transport."""
    transport = sftp.get_channel().get_transport()
    sftp.close()
    transport.close()
