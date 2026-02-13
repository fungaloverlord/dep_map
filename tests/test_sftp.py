"""Live SFTP integration tests — requires test server at 10.0.0.10."""

import os

import pytest

from sftp_client import connect, walk_remote, read_file, close


# Skip if SFTP server is not reachable
pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_SFTP_TESTS", "0") == "1",
    reason="SFTP tests skipped (set SKIP_SFTP_TESTS=0 to enable)",
)


@pytest.fixture(scope="module")
def sftp():
    """Establish SFTP connection for the test module."""
    client = connect()
    yield client
    close(client)


class TestConnect:
    def test_connection(self, sftp):
        assert sftp is not None

    def test_listdir(self, sftp):
        entries = sftp.listdir_attr(".")
        assert isinstance(entries, list)


class TestWalkRemote:
    def test_walk_returns_list(self, sftp):
        # Bounded walk with max_depth=1 to avoid traversing the whole filesystem
        results = walk_remote(sftp, ".", extensions={".sas"}, max_depth=1)
        assert isinstance(results, list)

    def test_walk_all_extensions(self, sftp):
        results = walk_remote(sftp, ".", extensions={".sas", ".txt"}, max_depth=0)
        assert isinstance(results, list)
        for entry in results:
            assert "path" in entry
            assert "stat" in entry
