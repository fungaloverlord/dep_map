"""Tests for parser.py — feed known SAS fragments, assert matches."""

from parser import (
    parse_table_writes,
    parse_table_reads,
    parse_includes,
    parse_macro_defs,
    parse_macro_calls,
    parse_libnames,
    parse_let_statements,
    parse_credentials,
)


# --- Table writes ---

class TestTableWrites:
    def test_data_step(self, patterns):
        src = "DATA mylib.customers;\n  SET work.raw;\nRUN;"
        results = parse_table_writes(src, patterns)
        assert len(results) == 1
        assert results[0]["libref"] == "mylib"
        assert results[0]["table"] == "customers"

    def test_data_null_excluded(self, patterns):
        src = "DATA _null_;\n  FILE print;\nRUN;"
        results = parse_table_writes(src, patterns)
        assert len(results) == 0

    def test_create_table(self, patterns):
        src = "PROC SQL;\n  CREATE TABLE ora.summary AS SELECT * FROM ora.detail;\nQUIT;"
        results = parse_table_writes(src, patterns)
        assert any(r["table"] == "summary" and r["libref"] == "ora" for r in results)

    def test_insert_into(self, patterns):
        src = "PROC SQL;\n  INSERT INTO sf.target (col1) VALUES ('x');\nQUIT;"
        results = parse_table_writes(src, patterns)
        assert any(r["table"] == "target" for r in results)

    def test_proc_append(self, patterns):
        src = "PROC APPEND BASE=mylib.master DATA=work.new; RUN;"
        results = parse_table_writes(src, patterns)
        assert any(r["table"] == "master" and r["libref"] == "mylib" for r in results)

    def test_passthrough_create(self, patterns):
        src = "EXECUTE(CREATE TABLE myschema.newtbl (id int))"
        results = parse_table_writes(src, patterns)
        assert any(r["table"] == "newtbl" for r in results)


# --- Table reads ---

class TestTableReads:
    def test_set(self, patterns):
        src = "DATA work.out;\n  SET mylib.input;\nRUN;"
        results = parse_table_reads(src, patterns)
        assert any(r["table"] == "input" and r["libref"] == "mylib" for r in results)

    def test_merge(self, patterns):
        src = "DATA work.out;\n  MERGE mylib.a mylib.b;\n  BY id;\nRUN;"
        results = parse_table_reads(src, patterns)
        tables = {r["table"] for r in results}
        assert "a" in tables and "b" in tables

    def test_from_clause(self, patterns):
        src = "PROC SQL;\n  SELECT * FROM ora.detail WHERE x=1;\nQUIT;"
        results = parse_table_reads(src, patterns)
        assert any(r["table"] == "detail" for r in results)

    def test_join(self, patterns):
        src = "PROC SQL;\n  SELECT * FROM ora.a JOIN ora.b ON a.id=b.id;\nQUIT;"
        results = parse_table_reads(src, patterns)
        tables = {r["table"] for r in results}
        assert "a" in tables and "b" in tables

    def test_passthrough_select(self, patterns):
        src = "EXECUTE(SELECT col1, col2 FROM myschema.sourcetbl WHERE x=1)"
        results = parse_table_reads(src, patterns)
        assert any(r["table"] == "sourcetbl" for r in results)


# --- Includes ---

class TestIncludes:
    def test_quoted_include(self, patterns):
        src = """%INCLUDE '/shared/macros/utils.sas';"""
        results = parse_includes(src, patterns)
        assert any(r["path"] == "/shared/macros/utils.sas" for r in results)

    def test_unquoted_include(self, patterns):
        src = "%include /prod/common/setup.sas;"
        results = parse_includes(src, patterns)
        assert any(r["path"] == "/prod/common/setup.sas" for r in results)


# --- Macro defs ---

class TestMacroDefs:
    def test_macro_def(self, patterns):
        src = "%MACRO load_data(dsn=);\n  /* body */\n%MEND;"
        results = parse_macro_defs(src, patterns)
        assert len(results) == 1
        assert results[0]["name"] == "load_data"


# --- Macro calls ---

class TestMacroCalls:
    def test_macro_call(self, patterns):
        src = "%load_data(dsn=mylib.input);"
        results = parse_macro_calls(src, patterns)
        assert any(r["name"] == "load_data" for r in results)

    def test_builtins_excluded(self, patterns):
        src = "%IF &x = 1 %THEN %DO;\n  %LET y = 2;\n%END;"
        results = parse_macro_calls(src, patterns)
        names = {r["name"] for r in results}
        assert "IF" not in names
        assert "THEN" not in names
        assert "DO" not in names
        assert "LET" not in names
        assert "END" not in names

    def test_datalab_connections_excluded(self, patterns):
        src = "%datalab_connections;"
        results = parse_macro_calls(src, patterns)
        names = {r["name"] for r in results}
        assert "datalab_connections" not in names


# --- LIBNAME ---

class TestLibnames:
    def test_oracle_libname(self, patterns):
        src = """LIBNAME myora ORACLE PATH='PROD' SCHEMA='DBO' USER=&uid PASSWORD=&pwd;"""
        results = parse_libnames(src, patterns)
        assert any(r["libref"] == "myora" and r["path"] == "PROD" and r["schema"] == "DBO" for r in results)

    def test_snowflake_libname(self, patterns):
        src = """LIBNAME sf SNOW SERVER='myacct.snowflakecomputing.com' DATABASE=&sf_database SCHEMA=&sf_schema;"""
        results = parse_libnames(src, patterns)
        assert any(r["libref"] == "sf" and r["database"] == "&sf_database" for r in results)

    def test_base_libname(self, patterns):
        src = """LIBNAME work '/data/saswork';"""
        results = parse_libnames(src, patterns)
        assert any(r["libref"] == "work" and r["path"] == "/data/saswork" for r in results)


# --- %LET ---

class TestLetStatements:
    def test_let_basic(self, patterns):
        src = "%LET myvar = hello_world;"
        results = parse_let_statements(src, patterns)
        assert len(results) == 1
        assert results[0]["variable"] == "myvar"
        assert results[0]["value"] == "hello_world"

    def test_let_multiple(self, patterns):
        src = "%LET db = PROD_DB;\n%LET schema = DBO;\n%LET db = DEV_DB;"
        results = parse_let_statements(src, patterns)
        assert len(results) == 3
        # Last value for db should be DEV_DB
        db_vals = [r for r in results if r["variable"] == "db"]
        assert db_vals[-1]["value"] == "DEV_DB"


# --- Credentials ---

class TestCredentials:
    def test_hardcoded_password(self, patterns):
        src = "LIBNAME myora ORACLE USER=jsmith PASSWORD=hunter2;"
        results = parse_credentials(src, patterns)
        assert any(r["value"] == "hunter2" for r in results)
        assert any(r["value"] == "jsmith" for r in results)

    def test_macro_var_safe(self, patterns):
        src = "LIBNAME myora ORACLE USER=&uid PASSWORD=&pwd;"
        results = parse_credentials(src, patterns)
        assert len(results) == 0

    def test_line_number(self, patterns):
        src = "line1\nline2\nPASSWORD=secret\nline4"
        results = parse_credentials(src, patterns)
        assert results[0]["line"] == 3
