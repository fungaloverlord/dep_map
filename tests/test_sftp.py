"""SFTP tests — unit tests for read_file, live integration tests for the rest."""

import os
from unittest.mock import MagicMock

import pytest

from sftp_client import connect, walk_remote, read_file, close


# ---------------------------------------------------------------------------
# Unit tests for read_file (no SFTP server needed)
# ---------------------------------------------------------------------------

class _FakeSFTPFile:
    """Minimal stand-in for paramiko.SFTPFile returned by sftp.open()."""

    def __init__(self, data):
        self._data = data

    def prefetch(self):
        pass

    def read(self):
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def _mock_sftp(file_data):
    """Return an sftp mock whose .open() yields a _FakeSFTPFile."""
    sftp = MagicMock()
    sftp.open.return_value = _FakeSFTPFile(file_data)
    return sftp


class TestReadFile:
    """Unit tests for read_file covering encoding edge cases and contract."""

    # -- basic happy paths ---------------------------------------------------

    def test_pure_ascii(self):
        result = read_file(_mock_sftp(b"data a; set b; run;"), "/prog.sas")
        assert result == "data a; set b; run;"

    def test_utf8_content(self):
        content = b"/* normal comment */\ndata work.out; set work.in; run;"
        result = read_file(_mock_sftp(content), "/prog.sas")
        assert result == content.decode("utf-8")

    def test_valid_multibyte_utf8_preserved(self):
        """Accented and CJK characters survive the round-trip."""
        raw = "/* commentaire français */\ndata été; set données; run;".encode("utf-8")
        result = read_file(_mock_sftp(raw), "/prog.sas")
        assert "français" in result
        assert "été" in result
        assert "données" in result

    # -- Windows-1252 / non-UTF-8 bytes (the production bug) -----------------

    def test_windows_1252_en_dash_in_comment(self):
        """0x96 is an en dash in Windows-1252 — the exact production failure."""
        raw = b"/* date range: 2020\x962023 */\ndata out; set in; run;"
        result = read_file(_mock_sftp(raw), "/prog.sas")
        assert isinstance(result, str)
        # 0x96 is invalid UTF-8; errors="replace" turns it into U+FFFD
        assert "\ufffd" in result
        assert "data out; set in; run;" in result

    def test_multiple_non_utf8_bytes(self):
        """Several Windows-1252 bytes that are invalid in UTF-8."""
        # 0x93 = left double quote, 0x94 = right double quote, 0x96 = en dash
        raw = b"/* \x93quoted\x94 value \x96 note */\n%put done;"
        result = read_file(_mock_sftp(raw), "/prog.sas")
        assert isinstance(result, str)
        assert "%put done;" in result

    def test_smart_quotes_in_sas_string_literal(self):
        """Windows-1252 smart quotes inside a SAS string value."""
        raw = b"title \x93Quarterly Report\x94;\nproc print; run;"
        result = read_file(_mock_sftp(raw), "/prog.sas")
        assert "Quarterly Report" in result
        assert "proc print; run;" in result

    def test_non_utf8_with_crlf_line_endings(self):
        """Windows CRLF endings combined with invalid bytes."""
        raw = b"/* range \x96 */\r\ndata x;\r\nset y;\r\nrun;"
        result = read_file(_mock_sftp(raw), "/prog.sas")
        assert "data x;" in result
        assert "set y;" in result
        assert "\r\n" in result

    # -- edge-case byte patterns ---------------------------------------------

    def test_empty_file_bytes(self):
        result = read_file(_mock_sftp(b""), "/empty.sas")
        assert result == ""

    def test_empty_file_str(self):
        """Paramiko returning an empty string."""
        result = read_file(_mock_sftp(""), "/empty.sas")
        assert result == ""

    def test_null_bytes(self):
        """Null bytes should survive as \\x00 in the decoded string."""
        raw = b"data x;\x00 set y; run;"
        result = read_file(_mock_sftp(raw), "/prog.sas")
        assert isinstance(result, str)
        assert "\x00" in result
        assert "data x;" in result
        assert "set y; run;" in result

    def test_only_invalid_bytes(self):
        """A file containing no valid UTF-8 at all still returns a string."""
        raw = bytes(range(0x80, 0xA0))  # all invalid UTF-8 continuation bytes
        result = read_file(_mock_sftp(raw), "/garbage.sas")
        assert isinstance(result, str)
        assert all(c == "\ufffd" for c in result)

    def test_every_byte_value(self):
        """All 256 single-byte values: must not raise, must return str."""
        raw = bytes(range(256))
        result = read_file(_mock_sftp(raw), "/allbytes.sas")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_truncated_utf8_at_eof(self):
        """Incomplete multibyte sequence at end of file."""
        # \xc3 is the first byte of a 2-byte UTF-8 sequence (e.g. \xc3\xa9 = é)
        raw = b"data x; run;\xc3"
        result = read_file(_mock_sftp(raw), "/prog.sas")
        assert isinstance(result, str)
        assert "data x; run;" in result
        assert result.endswith("\ufffd")

    def test_mixed_valid_multibyte_and_invalid(self):
        """Valid UTF-8 multibyte chars adjacent to invalid bytes."""
        raw = "café".encode("utf-8") + b"\x96" + "naïve".encode("utf-8")
        result = read_file(_mock_sftp(raw), "/prog.sas")
        assert "caf\u00e9" in result
        assert "na\u00efve" in result
        assert "\ufffd" in result

    def test_utf8_bom(self):
        """UTF-8 BOM at start of file is preserved (common from Windows editors)."""
        raw = b"\xef\xbb\xbfdata x; set y; run;"
        result = read_file(_mock_sftp(raw), "/bom.sas")
        assert "data x; set y; run;" in result
        assert result.startswith("\ufeff")

    # -- Paramiko str-return path --------------------------------------------

    def test_paramiko_returns_str(self):
        """If Paramiko already decoded to str, read_file returns it as-is."""
        already_decoded = "/* already a string */\ndata x; run;"
        result = read_file(_mock_sftp(already_decoded), "/prog.sas")
        assert result == already_decoded

    def test_paramiko_returns_str_with_unicode(self):
        """Str path works for content that already contains unicode chars."""
        already_decoded = "/* commentaire \u2013 français */\ndata x; run;"
        result = read_file(_mock_sftp(already_decoded), "/prog.sas")
        assert result == already_decoded
        assert "\u2013" in result

    # -- contract: mode, prefetch, return type -------------------------------

    def test_opens_in_binary_mode(self):
        mock = _mock_sftp(b"x")
        read_file(mock, "/prog.sas")
        mock.open.assert_called_once_with("/prog.sas", "rb")

    def test_prefetch_called(self):
        """prefetch() must be called to ensure reliable binary reads."""
        fake_file = MagicMock()
        fake_file.read.return_value = b"data x; run;"
        fake_file.__enter__ = MagicMock(return_value=fake_file)
        fake_file.__exit__ = MagicMock(return_value=False)
        mock = MagicMock()
        mock.open.return_value = fake_file
        read_file(mock, "/prog.sas")
        fake_file.prefetch.assert_called_once()

    def test_path_passed_through(self):
        """The exact path argument is forwarded to sftp.open()."""
        mock = _mock_sftp(b"x")
        read_file(mock, "/some/deep/path/program.sas")
        mock.open.assert_called_once_with("/some/deep/path/program.sas", "rb")

    def test_return_type_always_str(self):
        """Regardless of input type, return value is always str."""
        for data in [b"ascii", b"\x96\x93\x94", b"", b"\xc3\xa9", "already str"]:
            result = read_file(_mock_sftp(data), "/f.sas")
            assert isinstance(result, str), f"Expected str for input {data!r}, got {type(result)}"


# ---------------------------------------------------------------------------
# Live integration tests — requires test server at 10.0.0.10
# ---------------------------------------------------------------------------

_skip_sftp = pytest.mark.skipif(
    os.environ.get("SKIP_SFTP_TESTS", "0") == "1",
    reason="SFTP tests skipped (set SKIP_SFTP_TESTS=0 to enable)",
)


@pytest.fixture(scope="module")
def sftp():
    """Establish SFTP connection for the test module."""
    client = connect()
    yield client
    close(client)


@_skip_sftp
class TestConnect:
    def test_connection(self, sftp):
        assert sftp is not None

    def test_listdir(self, sftp):
        entries = sftp.listdir_attr(".")
        assert isinstance(entries, list)


@_skip_sftp
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
