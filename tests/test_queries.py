"""Tests for queries.py — seed DB with known graph, assert transitive results."""

import json

import pandas as pd

from db import init_db, upsert_programs, upsert_table_operations, upsert_program_dependencies
from queries import downstream_impact, upstream_dependencies, table_impact, credential_report


def _make_program(path, cred_findings=None):
    return {
        "program_path": path,
        "file_size": 100,
        "file_mtime": 1700000000,
        "file_atime": 1700000000,
        "file_uid": 1000,
        "file_gid": 1000,
        "file_mode": 33188,
        "owner": "joy",
        "scan_timestamp": "2024-01-01T00:00:00",
        "credential_findings": json.dumps(cred_findings) if cred_findings else None,
    }


def _seed_graph(conn):
    """Create a test graph:
    A writes table X
    B reads table X, writes table Y
    C reads table Y
    D includes E (program dependency)
    """
    programs = pd.DataFrame([
        _make_program("/a.sas"),
        _make_program("/b.sas"),
        _make_program("/c.sas"),
        _make_program("/d.sas"),
        _make_program("/e.sas"),
        _make_program("/f.sas", cred_findings=["[10] PASSWORD=hunter2"]),
    ])
    upsert_programs(conn, programs)
    conn.commit()

    # A writes X
    upsert_table_operations(conn, "/a.sas", pd.DataFrame([{
        "program_path": "/a.sas",
        "table_name": "schema.x",
        "database_type": "oracle",
        "operation_type": "create",
        "source_line": 1,
        "in_scope": 1,
    }]))

    # B reads X, writes Y
    upsert_table_operations(conn, "/b.sas", pd.DataFrame([
        {
            "program_path": "/b.sas",
            "table_name": "schema.x",
            "database_type": "oracle",
            "operation_type": "read",
            "source_line": 1,
            "in_scope": 1,
        },
        {
            "program_path": "/b.sas",
            "table_name": "schema.y",
            "database_type": "oracle",
            "operation_type": "create",
            "source_line": 5,
            "in_scope": 1,
        },
    ]))

    # C reads Y
    upsert_table_operations(conn, "/c.sas", pd.DataFrame([{
        "program_path": "/c.sas",
        "table_name": "schema.y",
        "database_type": "oracle",
        "operation_type": "read",
        "source_line": 1,
        "in_scope": 1,
    }]))

    # D includes E
    upsert_program_dependencies(conn, "/d.sas", pd.DataFrame([{
        "source_program": "/d.sas",
        "target_program": "/e.sas",
        "dependency_type": "include",
    }]))


class TestDownstreamImpact:
    def test_direct(self, db_conn):
        _seed_graph(db_conn)
        result = downstream_impact(db_conn, "/a.sas")
        paths = set(result["program_path"])
        assert "/b.sas" in paths

    def test_transitive(self, db_conn):
        _seed_graph(db_conn)
        result = downstream_impact(db_conn, "/a.sas")
        paths = set(result["program_path"])
        # A → X → B → Y → C (transitive)
        assert "/c.sas" in paths

    def test_no_self(self, db_conn):
        _seed_graph(db_conn)
        result = downstream_impact(db_conn, "/a.sas")
        paths = set(result["program_path"])
        assert "/a.sas" not in paths

    def test_leaf_no_impact(self, db_conn):
        _seed_graph(db_conn)
        result = downstream_impact(db_conn, "/c.sas")
        assert result.empty

    def test_program_dependency(self, db_conn):
        _seed_graph(db_conn)
        # E is included by D, so changing E impacts D
        result = downstream_impact(db_conn, "/e.sas")
        paths = set(result["program_path"])
        assert "/d.sas" in paths


class TestUpstreamDependencies:
    def test_direct(self, db_conn):
        _seed_graph(db_conn)
        result = upstream_dependencies(db_conn, "/b.sas")
        paths = set(result["program_path"])
        assert "/a.sas" in paths

    def test_transitive(self, db_conn):
        _seed_graph(db_conn)
        result = upstream_dependencies(db_conn, "/c.sas")
        paths = set(result["program_path"])
        # C reads Y ← B writes Y, B reads X ← A writes X
        assert "/a.sas" in paths
        assert "/b.sas" in paths

    def test_root_no_upstream(self, db_conn):
        _seed_graph(db_conn)
        result = upstream_dependencies(db_conn, "/a.sas")
        assert result.empty

    def test_program_dependency_upstream(self, db_conn):
        _seed_graph(db_conn)
        # D includes E, so E is upstream of D
        result = upstream_dependencies(db_conn, "/d.sas")
        paths = set(result["program_path"])
        assert "/e.sas" in paths


class TestTableImpact:
    def test_all_users(self, db_conn):
        _seed_graph(db_conn)
        result = table_impact(db_conn, "schema.x")
        paths = set(result["program_path"])
        assert "/a.sas" in paths  # writes
        assert "/b.sas" in paths  # reads

    def test_nonexistent_table(self, db_conn):
        _seed_graph(db_conn)
        result = table_impact(db_conn, "no.such.table")
        assert result.empty


class TestCredentialReport:
    def test_finds_flagged(self, db_conn):
        _seed_graph(db_conn)
        result = credential_report(db_conn)
        paths = set(result["program_path"])
        assert "/f.sas" in paths
        assert len(result) == 1

    def test_clean_excluded(self, db_conn):
        _seed_graph(db_conn)
        result = credential_report(db_conn)
        paths = set(result["program_path"])
        assert "/a.sas" not in paths
