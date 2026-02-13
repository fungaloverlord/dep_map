"""Pipeline orchestrator — connects all modules into the scan pipeline."""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from parser import (
    load_patterns,
    parse_table_writes,
    parse_table_reads,
    parse_includes,
    parse_macro_defs,
    parse_macro_calls,
    parse_libnames,
    parse_let_statements,
    parse_credentials,
)
from resolver import (
    resolve_macro_vars,
    apply_datalab_connections,
    resolve_table_name,
    detect_snowflake_write_scope,
    build_libname_map,
)
from db import (
    init_db,
    get_scan_state,
    upsert_programs,
    upsert_table_operations,
    upsert_program_dependencies,
    upsert_libname_mappings,
    clear_program,
)
from sftp_client import connect, walk_remote, read_file, close

log = logging.getLogger(__name__)


def load_config(config_path=None):
    """Load config.yaml and return dict."""
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def scan(config_path=None, full=False):
    """Run the full scan pipeline.

    1. Load config, compile patterns, init DB
    2. Connect SFTP
    3. Walk macro dir first → build macro catalog
    4. Walk all scan roots → parse each .sas file
    5. Write results to SQLite
    6. Clear removed files
    7. Print summary
    """
    config = load_config(config_path)
    project_root = Path(__file__).parent
    patterns = load_patterns(project_root / "patterns.yaml")

    db_path = config.get("database", "sas_mapper.db")
    if not os.path.isabs(db_path):
        db_path = str(project_root / db_path)
    conn = init_db(db_path)

    scan_state = {} if full else get_scan_state(conn)
    known_librefs = config.get("known_librefs", {}) or {}
    datalab_defaults = config.get("datalab_defaults", {})
    scope_dbs = set(config.get("snowflake_write_scope", []))
    extensions = set(config.get("extensions", [".sas"]))

    sftp = connect()
    now = datetime.now(timezone.utc).isoformat()

    # Track all files seen on SFTP for cleanup
    all_remote_paths = set()
    macro_catalog = {}  # macro_name (lower) → file_path

    stats = {"scanned": 0, "skipped": 0, "errors": 0, "removed": 0}

    try:
        # --- Phase 1: Scan macro directory ---
        macro_dir = config.get("macro_directory")
        if macro_dir:
            log.info("Scanning macro directory: %s", macro_dir)
            macro_files = walk_remote(sftp, macro_dir, extensions)
            for entry in macro_files:
                path = entry["path"]
                all_remote_paths.add(path)
                mtime = entry["stat"].st_mtime

                if not full and path in scan_state and scan_state[path] == mtime:
                    stats["skipped"] += 1
                    # Still need macro catalog from previously parsed macros
                    # We'll rebuild from DB or re-parse — for simplicity, always parse macro dir
                    pass

                try:
                    source = read_file(sftp, path)
                except Exception as e:
                    log.error("Failed to read %s: %s", path, e)
                    stats["errors"] += 1
                    continue

                # Parse macro definitions for catalog
                macro_defs = parse_macro_defs(source, patterns)
                for md in macro_defs:
                    macro_catalog[md["name"].lower()] = path

                # Also parse the macro file for everything else
                _process_file(
                    conn, sftp, path, entry["stat"], source, patterns,
                    macro_catalog, known_librefs, datalab_defaults, scope_dbs, now,
                )
                stats["scanned"] += 1

        # --- Phase 2: Scan all roots ---
        for root in config.get("scan_roots", []):
            log.info("Scanning root: %s", root)
            files = walk_remote(sftp, root, extensions)
            for entry in files:
                path = entry["path"]

                # Skip if already processed as macro file
                if path in all_remote_paths:
                    continue
                all_remote_paths.add(path)

                mtime = entry["stat"].st_mtime
                if not full and path in scan_state and scan_state[path] == mtime:
                    stats["skipped"] += 1
                    continue

                try:
                    source = read_file(sftp, path)
                except Exception as e:
                    log.error("Failed to read %s: %s", path, e)
                    stats["errors"] += 1
                    continue

                _process_file(
                    conn, sftp, path, entry["stat"], source, patterns,
                    macro_catalog, known_librefs, datalab_defaults, scope_dbs, now,
                )
                stats["scanned"] += 1

        # --- Phase 3: Clean removed files ---
        for old_path in scan_state:
            if old_path not in all_remote_paths:
                clear_program(conn, old_path)
                stats["removed"] += 1
                log.info("Removed deleted file: %s", old_path)

    finally:
        close(sftp)
        conn.close()

    log.info(
        "Scan complete: %d scanned, %d skipped, %d errors, %d removed",
        stats["scanned"], stats["skipped"], stats["errors"], stats["removed"],
    )
    return stats


def _process_file(conn, sftp, path, stat_attrs, source, patterns,
                   macro_catalog, known_librefs, datalab_defaults, scope_dbs, now):
    """Parse and store results for a single SAS file."""
    try:
        # Parse %LET statements → resolve macro vars
        let_stmts = parse_let_statements(source, patterns)
        macro_vars = resolve_macro_vars(let_stmts)

        # Check for %datalab_connections → apply SF defaults
        macro_vars = apply_datalab_connections(source, macro_vars, datalab_defaults)

        # Parse LIBNAME statements → build libname map
        parsed_libnames = parse_libnames(source, patterns)
        libname_map = build_libname_map(parsed_libnames, macro_vars)

        # Store libname mappings
        if parsed_libnames:
            lib_records = []
            for entry in parsed_libnames:
                pname = entry.get("pattern_name", "")
                engine = "unknown"
                if "oracle" in pname:
                    engine = "oracle"
                elif "snowflake" in pname:
                    engine = "snowflake"
                elif "base" in pname:
                    engine = "base"
                lib_records.append({
                    "libref": entry.get("libref", "").lower(),
                    "engine": engine,
                    "source": "parsed",
                })
            if lib_records:
                upsert_libname_mappings(conn, pd.DataFrame(lib_records))

        # Parse table writes
        writes = parse_table_writes(source, patterns)
        write_records = []
        for w in writes:
            libref = w.get("libref") or w.get("schema")
            table = w.get("table")
            qualified, db_type = resolve_table_name(
                libref, table, macro_vars, libname_map, known_librefs
            )
            in_scope = 1
            if db_type == "snowflake":
                in_scope = 1 if detect_snowflake_write_scope(qualified, db_type, scope_dbs) else 0
            write_records.append({
                "program_path": path,
                "table_name": qualified,
                "database_type": db_type,
                "operation_type": "create",
                "source_line": w["line"],
                "in_scope": in_scope,
            })

        # Parse table reads
        reads = parse_table_reads(source, patterns)
        read_records = []
        for r in reads:
            libref = r.get("libref") or r.get("schema")
            table = r.get("table")
            qualified, db_type = resolve_table_name(
                libref, table, macro_vars, libname_map, known_librefs
            )
            read_records.append({
                "program_path": path,
                "table_name": qualified,
                "database_type": db_type,
                "operation_type": "read",
                "source_line": r["line"],
                "in_scope": 1,
            })

        # Combine and store table operations
        all_ops = write_records + read_records
        ops_df = pd.DataFrame(all_ops) if all_ops else pd.DataFrame()
        upsert_table_operations(conn, path, ops_df)

        # Parse program dependencies
        dep_records = []

        # %INCLUDE
        includes = parse_includes(source, patterns)
        for inc in includes:
            dep_records.append({
                "source_program": path,
                "target_program": inc["path"],
                "dependency_type": "include",
            })

        # Macro calls → resolve against catalog
        calls = parse_macro_calls(source, patterns)
        for call in calls:
            macro_name = call["name"].lower()
            if macro_name in macro_catalog:
                dep_records.append({
                    "source_program": path,
                    "target_program": macro_catalog[macro_name],
                    "dependency_type": "macro_call",
                })

        deps_df = pd.DataFrame(dep_records) if dep_records else pd.DataFrame()
        upsert_program_dependencies(conn, path, deps_df)

        # Parse credentials
        cred_findings = parse_credentials(source, patterns)
        cred_json = None
        if cred_findings:
            cred_json = json.dumps(
                [f"[{c['line']}] {c['pattern_name']}: {c['value']}" for c in cred_findings]
            )

        # Store program record
        prog_df = pd.DataFrame([{
            "program_path": path,
            "file_size": stat_attrs.st_size,
            "file_mtime": stat_attrs.st_mtime,
            "file_atime": stat_attrs.st_atime,
            "file_uid": stat_attrs.st_uid,
            "file_gid": stat_attrs.st_gid,
            "file_mode": stat_attrs.st_mode,
            "owner": str(stat_attrs.st_uid),
            "scan_timestamp": now,
            "credential_findings": cred_json,
        }])
        upsert_programs(conn, prog_df)
        conn.commit()

    except Exception as e:
        log.error("Error processing %s: %s", path, e)
        raise
