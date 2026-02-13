"""SQLite database layer — schema, upserts, scan state queries."""

import json
import sqlite3

import pandas as pd


def init_db(path):
    """Create SQLite database with schema. Returns connection."""
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS programs (
            program_path    TEXT PRIMARY KEY,
            file_size       INTEGER,
            file_mtime      INTEGER,
            file_atime      INTEGER,
            file_uid        INTEGER,
            file_gid        INTEGER,
            file_mode       INTEGER,
            owner           TEXT,
            scan_timestamp  TEXT NOT NULL,
            credential_findings TEXT
        );

        CREATE TABLE IF NOT EXISTS table_operations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            program_path    TEXT NOT NULL REFERENCES programs(program_path),
            table_name      TEXT NOT NULL,
            database_type   TEXT NOT NULL,
            operation_type  TEXT NOT NULL,
            source_line     INTEGER,
            in_scope        INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS program_dependencies (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_program  TEXT NOT NULL REFERENCES programs(program_path),
            target_program  TEXT NOT NULL,
            dependency_type TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS libname_mappings (
            libref          TEXT PRIMARY KEY,
            engine          TEXT NOT NULL,
            source          TEXT NOT NULL DEFAULT 'parsed'
        );

        CREATE INDEX IF NOT EXISTS idx_table_ops_program
            ON table_operations(program_path);
        CREATE INDEX IF NOT EXISTS idx_table_ops_table
            ON table_operations(table_name);
        CREATE INDEX IF NOT EXISTS idx_table_ops_type
            ON table_operations(operation_type);
        CREATE INDEX IF NOT EXISTS idx_deps_source
            ON program_dependencies(source_program);
        CREATE INDEX IF NOT EXISTS idx_deps_target
            ON program_dependencies(target_program);
    """)
    conn.commit()
    return conn


def get_scan_state(conn):
    """Return {program_path: mtime} for all previously scanned files."""
    cursor = conn.execute("SELECT program_path, file_mtime FROM programs")
    return {row[0]: row[1] for row in cursor.fetchall()}


def upsert_programs(conn, df):
    """Insert or replace program records from a DataFrame."""
    if df.empty:
        return
    df.to_sql("programs", conn, if_exists="append", index=False,
              method=_upsert_method("programs"))


def _upsert_method(table_name):
    """Return a callable for pandas to_sql that does INSERT OR REPLACE."""
    def method(pd_table, conn, keys, data_iter):
        cols = ", ".join(keys)
        placeholders = ", ".join(["?"] * len(keys))
        sql = f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"
        data = list(data_iter)
        conn.executemany(sql, data)
    return method


def upsert_table_operations(conn, program_path, df):
    """Replace all table operations for a given program path."""
    conn.execute("DELETE FROM table_operations WHERE program_path = ?", (program_path,))
    if not df.empty:
        df.to_sql("table_operations", conn, if_exists="append", index=False)
    conn.commit()


def upsert_program_dependencies(conn, program_path, df):
    """Replace all dependencies for a given source program."""
    conn.execute("DELETE FROM program_dependencies WHERE source_program = ?", (program_path,))
    if not df.empty:
        df.to_sql("program_dependencies", conn, if_exists="append", index=False)
    conn.commit()


def upsert_libname_mappings(conn, df):
    """Insert or replace libname mappings."""
    if df.empty:
        return
    df.to_sql("libname_mappings", conn, if_exists="append", index=False,
              method=_upsert_method("libname_mappings"))
    conn.commit()


def clear_program(conn, path):
    """Remove a program and all its related records (for deleted files)."""
    conn.execute("DELETE FROM table_operations WHERE program_path = ?", (path,))
    conn.execute("DELETE FROM program_dependencies WHERE source_program = ?", (path,))
    conn.execute("DELETE FROM programs WHERE program_path = ?", (path,))
    conn.commit()
