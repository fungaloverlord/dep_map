"""Tests for db.py — schema, upserts, scan state."""

import json

import pandas as pd

from db import (
    init_db,
    get_scan_state,
    upsert_programs,
    upsert_table_operations,
    upsert_program_dependencies,
    upsert_libname_mappings,
    clear_program,
)


class TestInitDb:
    def test_creates_tables(self, db_conn):
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {row[0] for row in cursor.fetchall()}
        assert "programs" in tables
        assert "table_operations" in tables
        assert "program_dependencies" in tables
        assert "libname_mappings" in tables

    def test_idempotent(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        conn1 = init_db(db_path)
        conn1.close()
        conn2 = init_db(db_path)
        cursor = conn2.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        assert len(cursor.fetchall()) >= 4
        conn2.close()


class TestScanState:
    def test_empty(self, db_conn):
        state = get_scan_state(db_conn)
        assert state == {}

    def test_returns_mtime(self, db_conn):
        df = pd.DataFrame([{
            "program_path": "/test/a.sas",
            "file_size": 100,
            "file_mtime": 1700000000,
            "file_atime": 1700000000,
            "file_uid": 1000,
            "file_gid": 1000,
            "file_mode": 33188,
            "owner": "joy",
            "scan_timestamp": "2024-01-01T00:00:00",
            "credential_findings": None,
        }])
        upsert_programs(db_conn, df)
        db_conn.commit()
        state = get_scan_state(db_conn)
        assert state["/test/a.sas"] == 1700000000


class TestUpsertPrograms:
    def test_insert_and_replace(self, db_conn):
        df1 = pd.DataFrame([{
            "program_path": "/test/b.sas",
            "file_size": 200,
            "file_mtime": 1700000001,
            "file_atime": 1700000001,
            "file_uid": 1000,
            "file_gid": 1000,
            "file_mode": 33188,
            "owner": "joy",
            "scan_timestamp": "2024-01-01T00:00:00",
            "credential_findings": None,
        }])
        upsert_programs(db_conn, df1)
        db_conn.commit()

        # Update with new size
        df2 = pd.DataFrame([{
            "program_path": "/test/b.sas",
            "file_size": 300,
            "file_mtime": 1700000002,
            "file_atime": 1700000002,
            "file_uid": 1000,
            "file_gid": 1000,
            "file_mode": 33188,
            "owner": "joy",
            "scan_timestamp": "2024-01-02T00:00:00",
            "credential_findings": None,
        }])
        upsert_programs(db_conn, df2)
        db_conn.commit()

        cursor = db_conn.execute(
            "SELECT file_size FROM programs WHERE program_path = '/test/b.sas'"
        )
        assert cursor.fetchone()[0] == 300


class TestUpsertTableOperations:
    def test_delete_then_insert(self, db_conn):
        # Seed a program first
        df_prog = pd.DataFrame([{
            "program_path": "/test/c.sas",
            "file_size": 100,
            "file_mtime": 1700000000,
            "file_atime": 1700000000,
            "file_uid": 1000,
            "file_gid": 1000,
            "file_mode": 33188,
            "owner": "joy",
            "scan_timestamp": "2024-01-01T00:00:00",
            "credential_findings": None,
        }])
        upsert_programs(db_conn, df_prog)
        db_conn.commit()

        df_ops1 = pd.DataFrame([{
            "program_path": "/test/c.sas",
            "table_name": "schema.old_table",
            "database_type": "oracle",
            "operation_type": "read",
            "source_line": 10,
            "in_scope": 1,
        }])
        upsert_table_operations(db_conn, "/test/c.sas", df_ops1)

        df_ops2 = pd.DataFrame([{
            "program_path": "/test/c.sas",
            "table_name": "schema.new_table",
            "database_type": "oracle",
            "operation_type": "create",
            "source_line": 20,
            "in_scope": 1,
        }])
        upsert_table_operations(db_conn, "/test/c.sas", df_ops2)

        cursor = db_conn.execute(
            "SELECT table_name FROM table_operations WHERE program_path = '/test/c.sas'"
        )
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "schema.new_table"


class TestUpsertProgramDependencies:
    def test_delete_then_insert(self, db_conn):
        df_prog = pd.DataFrame([{
            "program_path": "/test/d.sas",
            "file_size": 100,
            "file_mtime": 1700000000,
            "file_atime": 1700000000,
            "file_uid": 1000,
            "file_gid": 1000,
            "file_mode": 33188,
            "owner": "joy",
            "scan_timestamp": "2024-01-01T00:00:00",
            "credential_findings": None,
        }])
        upsert_programs(db_conn, df_prog)
        db_conn.commit()

        df_deps = pd.DataFrame([{
            "source_program": "/test/d.sas",
            "target_program": "/macros/util.sas",
            "dependency_type": "macro_call",
        }])
        upsert_program_dependencies(db_conn, "/test/d.sas", df_deps)

        cursor = db_conn.execute(
            "SELECT target_program FROM program_dependencies WHERE source_program = '/test/d.sas'"
        )
        assert cursor.fetchone()[0] == "/macros/util.sas"


class TestUpsertLibnameMappings:
    def test_insert(self, db_conn):
        df = pd.DataFrame([{
            "libref": "myora",
            "engine": "oracle",
            "source": "parsed",
        }])
        upsert_libname_mappings(db_conn, df)

        cursor = db_conn.execute("SELECT engine FROM libname_mappings WHERE libref = 'myora'")
        assert cursor.fetchone()[0] == "oracle"


class TestClearProgram:
    def test_removes_all_related(self, db_conn):
        # Insert program
        df_prog = pd.DataFrame([{
            "program_path": "/test/e.sas",
            "file_size": 100,
            "file_mtime": 1700000000,
            "file_atime": 1700000000,
            "file_uid": 1000,
            "file_gid": 1000,
            "file_mode": 33188,
            "owner": "joy",
            "scan_timestamp": "2024-01-01T00:00:00",
            "credential_findings": None,
        }])
        upsert_programs(db_conn, df_prog)
        db_conn.commit()

        # Insert ops and deps
        df_ops = pd.DataFrame([{
            "program_path": "/test/e.sas",
            "table_name": "schema.tbl",
            "database_type": "oracle",
            "operation_type": "read",
            "source_line": 5,
            "in_scope": 1,
        }])
        upsert_table_operations(db_conn, "/test/e.sas", df_ops)

        df_deps = pd.DataFrame([{
            "source_program": "/test/e.sas",
            "target_program": "/test/f.sas",
            "dependency_type": "include",
        }])
        upsert_program_dependencies(db_conn, "/test/e.sas", df_deps)

        # Now clear
        clear_program(db_conn, "/test/e.sas")

        assert db_conn.execute("SELECT COUNT(*) FROM programs WHERE program_path = '/test/e.sas'").fetchone()[0] == 0
        assert db_conn.execute("SELECT COUNT(*) FROM table_operations WHERE program_path = '/test/e.sas'").fetchone()[0] == 0
        assert db_conn.execute("SELECT COUNT(*) FROM program_dependencies WHERE source_program = '/test/e.sas'").fetchone()[0] == 0
