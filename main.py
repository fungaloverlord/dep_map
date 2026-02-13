"""CLI entry point for SAS Mapper."""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

from scanner import scan, load_config
from db import init_db
from queries import downstream_impact, upstream_dependencies, table_impact, credential_report


def _get_db_conn(config_path=None):
    """Open a read-only connection to the scan database."""
    config = load_config(config_path)
    db_path = config.get("database", "sas_mapper.db")
    if not os.path.isabs(db_path):
        db_path = str(Path(__file__).parent / db_path)
    return init_db(db_path)


def cmd_scan(args):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    stats = scan(config_path=args.config, full=args.full)
    print(f"\nScan complete:")
    print(f"  Scanned: {stats['scanned']}")
    print(f"  Skipped: {stats['skipped']} (unchanged)")
    print(f"  Errors:  {stats['errors']}")
    print(f"  Removed: {stats['removed']} (deleted from SFTP)")


def cmd_impact(args):
    conn = _get_db_conn(args.config)
    try:
        result = downstream_impact(conn, args.path)
        if result.empty:
            print(f"No downstream impact found for {args.path}")
        else:
            print(f"Downstream impact for {args.path}:\n")
            print(result.to_string(index=False))
    finally:
        conn.close()


def cmd_upstream(args):
    conn = _get_db_conn(args.config)
    try:
        result = upstream_dependencies(conn, args.path)
        if result.empty:
            print(f"No upstream dependencies found for {args.path}")
        else:
            print(f"Upstream dependencies for {args.path}:\n")
            print(result.to_string(index=False))
    finally:
        conn.close()


def cmd_table(args):
    conn = _get_db_conn(args.config)
    try:
        result = table_impact(conn, args.name)
        if result.empty:
            print(f"No programs found for table {args.name}")
        else:
            print(f"Programs using table {args.name}:\n")
            print(result.to_string(index=False))
    finally:
        conn.close()


def cmd_credentials(args):
    conn = _get_db_conn(args.config)
    try:
        result = credential_report(conn)
        if result.empty:
            print("No hardcoded credentials found.")
        else:
            print(f"Programs with hardcoded credentials:\n")
            for _, row in result.iterrows():
                print(f"  {row['program_path']}")
                print(f"    Findings: {row['credential_findings']}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        prog="sas_mapper",
        description="SAS program dependency mapper and impact analyzer",
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml")

    sub = parser.add_subparsers(dest="command")
    sub.required = True

    # scan
    p_scan = sub.add_parser("scan", help="Scan SFTP for SAS programs")
    p_scan.add_argument("--full", action="store_true", help="Full rescan (ignore mtime)")
    p_scan.set_defaults(func=cmd_scan)

    # impact (downstream)
    p_impact = sub.add_parser("impact", help="Show downstream impact of a program")
    p_impact.add_argument("path", help="Program path (as stored in DB)")
    p_impact.set_defaults(func=cmd_impact)

    # upstream
    p_upstream = sub.add_parser("upstream", help="Show upstream dependencies of a program")
    p_upstream.add_argument("path", help="Program path (as stored in DB)")
    p_upstream.set_defaults(func=cmd_upstream)

    # table
    p_table = sub.add_parser("table", help="Show programs that use a table")
    p_table.add_argument("name", help="Table name (schema-qualified)")
    p_table.set_defaults(func=cmd_table)

    # credentials
    p_cred = sub.add_parser("credentials", help="Show hardcoded credential findings")
    p_cred.set_defaults(func=cmd_credentials)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
