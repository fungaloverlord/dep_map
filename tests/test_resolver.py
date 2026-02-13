"""Tests for resolver.py — macro var resolution, table name resolution, SF scoping."""

from resolver import (
    resolve_macro_vars,
    apply_datalab_connections,
    resolve_table_name,
    detect_snowflake_write_scope,
    build_libname_map,
)


class TestResolveMacroVars:
    def test_basic(self):
        stmts = [
            {"variable": "db", "value": "PROD_DB"},
            {"variable": "schema", "value": "DBO"},
        ]
        result = resolve_macro_vars(stmts)
        assert result["db"] == "PROD_DB"
        assert result["schema"] == "DBO"

    def test_last_wins(self):
        stmts = [
            {"variable": "db", "value": "DEV_DB"},
            {"variable": "db", "value": "PROD_DB"},
        ]
        result = resolve_macro_vars(stmts)
        assert result["db"] == "PROD_DB"

    def test_case_insensitive_keys(self):
        stmts = [{"variable": "MyVar", "value": "hello"}]
        result = resolve_macro_vars(stmts)
        assert "myvar" in result


class TestDatalabConnections:
    def test_applies_defaults(self):
        source = "some code\n%datalab_connections;\nmore code"
        macro_vars = {}
        result = apply_datalab_connections(source, macro_vars)
        assert result["sf_database"] == "LIS_DTALAB_WRKGRP_SPC_DB"
        assert result["sf_schema"] == "DL_T1_ILS_ANALYTICS"
        assert result["sf_database_old"] == "ILS_DATALAB_SBX_DB"
        assert result["sf_schema_old"] == "DATALAB_ILSNP"

    def test_explicit_let_takes_precedence(self):
        source = "%datalab_connections;"
        macro_vars = {"sf_database": "MY_CUSTOM_DB"}
        result = apply_datalab_connections(source, macro_vars)
        assert result["sf_database"] == "MY_CUSTOM_DB"

    def test_no_macro_no_change(self):
        source = "just regular SAS code"
        macro_vars = {"x": "1"}
        result = apply_datalab_connections(source, macro_vars)
        assert "sf_database" not in result


class TestResolveTableName:
    def test_oracle_libname(self):
        libname_map = {
            "myora": {"engine": "oracle", "path": "PROD", "schema": "DBO"},
        }
        name, db_type = resolve_table_name("myora", "customers", {}, libname_map, {})
        assert name == "DBO.customers"
        assert db_type == "oracle"

    def test_snowflake_libname(self):
        libname_map = {
            "sf": {
                "engine": "snowflake",
                "database": "LIS_DTALAB_WRKGRP_SPC_DB",
                "schema": "DL_T1_ILS_ANALYTICS",
            },
        }
        name, db_type = resolve_table_name("sf", "mytable", {}, libname_map, {})
        assert name == "LIS_DTALAB_WRKGRP_SPC_DB.DL_T1_ILS_ANALYTICS.mytable"
        assert db_type == "snowflake"

    def test_known_libref_fallback(self):
        known = {"prodlib": "oracle"}
        name, db_type = resolve_table_name("prodlib", "tbl", {}, {}, known)
        assert name == "prodlib.tbl"
        assert db_type == "oracle"

    def test_unknown_libref(self):
        name, db_type = resolve_table_name("mystery", "tbl", {}, {}, {})
        assert name == "mystery.tbl"
        assert db_type == "unknown"

    def test_macro_var_in_libref(self):
        macro_vars = {"mylib": "prodora"}
        known = {"prodora": "oracle"}
        name, db_type = resolve_table_name("&mylib", "tbl", macro_vars, {}, known)
        assert name == "prodora.tbl"
        assert db_type == "oracle"

    def test_unresolved_macro_var(self):
        name, db_type = resolve_table_name("&unknown_lib", "tbl", {}, {}, {})
        assert "unknown" in name
        assert db_type == "unknown"

    def test_work_library(self):
        name, db_type = resolve_table_name("work", "temp", {}, {}, {})
        assert name == "work.temp"
        assert db_type == "work"

    def test_none_handling(self):
        name, db_type = resolve_table_name(None, None, {}, {}, {})
        assert "unknown" in name
        assert db_type == "unknown"


class TestSnowflakeWriteScope:
    def test_in_scope(self):
        assert detect_snowflake_write_scope(
            "LIS_DTALAB_WRKGRP_SPC_DB.DL_T1_ILS_ANALYTICS.tbl", "snowflake"
        )

    def test_datalab_ilsnp_in_scope(self):
        assert detect_snowflake_write_scope(
            "DATALAB_ILSNP.schema.tbl", "snowflake"
        )

    def test_out_of_scope(self):
        assert not detect_snowflake_write_scope(
            "ILS_DATALAB_SBX_DB.schema.tbl", "snowflake"
        )

    def test_not_snowflake(self):
        assert not detect_snowflake_write_scope(
            "LIS_DTALAB_WRKGRP_SPC_DB.schema.tbl", "oracle"
        )


class TestBuildLibnameMap:
    def test_oracle(self):
        parsed = [{"pattern_name": "libname_oracle", "libref": "ORA", "path": "PROD", "schema": "DBO"}]
        result = build_libname_map(parsed, {})
        assert result["ora"]["engine"] == "oracle"
        assert result["ora"]["schema"] == "DBO"

    def test_snowflake_with_macro_vars(self):
        parsed = [{
            "pattern_name": "libname_snowflake",
            "libref": "SF",
            "database": "&sf_database",
            "schema": "&sf_schema",
        }]
        macro_vars = {
            "sf_database": "LIS_DTALAB_WRKGRP_SPC_DB",
            "sf_schema": "DL_T1_ILS_ANALYTICS",
        }
        result = build_libname_map(parsed, macro_vars)
        assert result["sf"]["engine"] == "snowflake"
        assert result["sf"]["database"] == "LIS_DTALAB_WRKGRP_SPC_DB"

    def test_base(self):
        parsed = [{"pattern_name": "libname_base", "libref": "MYDIR", "path": "/data/sasdata"}]
        result = build_libname_map(parsed, {})
        assert result["mydir"]["engine"] == "base"
        assert result["mydir"]["path"] == "/data/sasdata"
