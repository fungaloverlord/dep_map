# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SAS Mapper scans SAS programs on remote SFTP servers, extracts metadata, parses
code for database operations and inter-program dependencies, and stores results
for impact analysis.

Core question it answers: "If I change this program or table, what else is affected?"

## Key Design Decisions

- **SFTP access** via Paramiko
- **Storage**: pandas DataFrames → SQLite for dev. User handles final write to Snowflake. No storage abstraction layer.
- **Regex patterns** for SAS parsing live in a single dedicated file organized by category — parser code never contains inline regex
- **Err on inclusion**: if a table CAN be referenced by a program through logic or macro, it counts as impacting that program. Unresolvable refs marked "unknown", never skipped.
- **Incremental scanning**: track file mtime, only re-parse changed files; full rescan as fallback
- **Dependency graph**: derived at query time via recursive queries against stored data, not a separate structure

## LIBNAME Resolution Rules

- **Oracle**: LIBNAME statements are never dynamic — parse directly
- **Snowflake**: when `%datalab_connections` macro is present, these variables are set:
  - sf_database_old=ILS_DATALAB_SBX_DB, sf_schema_old=DATALAB_ILSNP
  - sf_database=LIS_DTALAB_WRKGRP_SPC_DB, sf_schema=DL_T1_ILS_ANALYTICS
- Snowflake writes only in scope for **LIS_DTALAB_WRKGRP_SPC_DB** or **DATALAB_ILSNP**
- For macro variable refs, use last `%let` value set in the program
- Unknown → mark "unknown", don't skip

## Scanning

- Multiple root directories configured, each walked recursively
- Dedicated macro directory scanned first to build macro catalog (name → file)
- Macro definitions only parsed from macro directory; all other dirs parsed for macro calls only
- Cross-directory dependencies tracked — all roots treated as one unified codebase

## What Gets Parsed

- Table writes: DATA step output, CREATE TABLE, INSERT INTO, PROC APPEND, pass-through EXECUTE
- Table reads: SET/MERGE/UPDATE in DATA steps, FROM/JOIN in PROC SQL, pass-through SELECT
- Program deps: %INCLUDE directives, macro calls resolved against macro catalog
- LIBNAME statements
- Hardcoded credentials (literal values where macro variable expected)

## Schema

Four tables: `programs` (path + metadata + scan_timestamp + credential_findings), `table_operations` (program → table + db_type + operation_type + source_line), `program_dependencies` (source → target + type), `libname_mappings` (libref → engine + source)
