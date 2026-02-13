"""Resolve macro variables, table names, LIBNAME mappings, and Snowflake scoping."""

import re


# Snowflake databases where writes are in scope
SNOWFLAKE_WRITE_SCOPE_DBS = {"LIS_DTALAB_WRKGRP_SPC_DB", "DATALAB_ILSNP"}

# Default datalab_connections macro variable values
DATALAB_DEFAULTS = {
    "sf_database_old": "ILS_DATALAB_SBX_DB",
    "sf_schema_old": "DATALAB_ILSNP",
    "sf_database": "LIS_DTALAB_WRKGRP_SPC_DB",
    "sf_schema": "DL_T1_ILS_ANALYTICS",
}


def resolve_macro_vars(let_statements):
    """Build macro variable map from parsed %LET statements.

    Last %LET value wins (per-file resolution only).
    Returns dict: variable_name (lowercased) → value.
    """
    macro_vars = {}
    for stmt in let_statements:
        macro_vars[stmt["variable"].lower()] = stmt["value"]
    return macro_vars


def apply_datalab_connections(source, macro_vars, datalab_defaults=None):
    """If %datalab_connections is present in source, merge SF default vars.

    The macro call sets known Snowflake variables. Explicit %LET in the file
    takes precedence (already in macro_vars), so we only set defaults for
    variables not already defined.
    """
    if datalab_defaults is None:
        datalab_defaults = DATALAB_DEFAULTS
    if re.search(r'(?i)%datalab_connections\b', source):
        for var, val in datalab_defaults.items():
            if var.lower() not in macro_vars:
                macro_vars[var.lower()] = val
    return macro_vars


def _substitute_macro_vars(text, macro_vars):
    """Replace &variable references with resolved values."""
    def replacer(m):
        var_name = m.group(1).lower()
        # Handle trailing dot (SAS macro var delimiter)
        return macro_vars.get(var_name, m.group(0))

    # Match &varname or &varname.
    return re.sub(r'&(\w+)\.?', replacer, text, flags=re.IGNORECASE)


def resolve_table_name(raw_libref, raw_table, macro_vars, libname_map, known_librefs):
    """Resolve a lib.table reference to (qualified_name, database_type).

    Steps:
    1. Substitute macro variables in libref and table name
    2. Look up libref in libname_map (parsed LIBNAME statements)
    3. Fall back to known_librefs (from config)
    4. If still unresolved, return ("unknown.<table>", "unknown")
    """
    if raw_libref is None:
        raw_libref = "unknown"
    if raw_table is None:
        raw_table = "unknown"

    libref = _substitute_macro_vars(raw_libref, macro_vars).lower()
    table = _substitute_macro_vars(raw_table, macro_vars).lower()

    # Check if libref still has unresolved macro var
    if "&" in libref:
        return f"unknown.{table}", "unknown"

    # Look up in parsed LIBNAME map first
    if libref in libname_map:
        mapping = libname_map[libref]
        engine = mapping["engine"]
        if engine == "oracle":
            schema = mapping.get("schema", libref)
            return f"{schema}.{table}", "oracle"
        elif engine == "snowflake":
            db = _substitute_macro_vars(mapping.get("database", ""), macro_vars)
            schema = _substitute_macro_vars(mapping.get("schema", ""), macro_vars)
            return f"{db}.{schema}.{table}", "snowflake"
        else:
            return f"{libref}.{table}", engine

    # Fall back to known librefs from config
    if libref in known_librefs:
        engine = known_librefs[libref]
        return f"{libref}.{table}", engine

    # Work library — local
    if libref == "work":
        return f"work.{table}", "work"

    return f"{libref}.{table}", "unknown"


def detect_snowflake_write_scope(qualified_name, db_type, scope_dbs=None):
    """Check if a Snowflake write targets an in-scope database.

    Only writes to LIS_DTALAB_WRKGRP_SPC_DB or DATALAB_ILSNP are in scope.
    """
    if scope_dbs is None:
        scope_dbs = SNOWFLAKE_WRITE_SCOPE_DBS
    if db_type != "snowflake":
        return False
    parts = qualified_name.split(".")
    if len(parts) >= 1:
        db_name = parts[0].upper()
        return db_name in {s.upper() for s in scope_dbs}
    return False


def build_libname_map(parsed_libnames, macro_vars):
    """Build a libref → {engine, schema/database/path} map from parsed LIBNAME results."""
    libname_map = {}
    for entry in parsed_libnames:
        name = entry.get("pattern_name", "")
        libref = entry.get("libref", "").lower()
        if "oracle" in name:
            libname_map[libref] = {
                "engine": "oracle",
                "path": entry.get("path", ""),
                "schema": entry.get("schema", ""),
            }
        elif "snowflake" in name:
            db = entry.get("database", "")
            schema = entry.get("schema", "")
            # Resolve macro vars in database/schema
            db = _substitute_macro_vars(db, macro_vars)
            schema = _substitute_macro_vars(schema, macro_vars)
            libname_map[libref] = {
                "engine": "snowflake",
                "database": db,
                "schema": schema,
            }
        elif "base" in name:
            libname_map[libref] = {
                "engine": "base",
                "path": entry.get("path", ""),
            }
    return libname_map
