"""Impact analysis queries — recursive CTEs against SQLite."""

import pandas as pd


def downstream_impact(conn, program_path):
    """Find all programs and tables downstream of a given program.

    Logic: find tables this program writes → programs that read those tables →
    tables those programs write → etc. Also follows program dependencies.
    """
    sql = """
    WITH RECURSIVE
    -- Seed: the starting program
    impacted_programs(program_path, depth) AS (
        SELECT ?, 0

        UNION

        -- Programs that read tables written by an impacted program
        SELECT DISTINCT r.program_path, ip.depth + 1
        FROM impacted_programs ip
        JOIN table_operations w ON w.program_path = ip.program_path AND w.operation_type = 'create'
        JOIN table_operations r ON r.table_name = w.table_name AND r.operation_type = 'read'
        WHERE r.program_path != ip.program_path
          AND ip.depth < 20

        UNION

        -- Programs that depend on (include/call) an impacted program
        SELECT DISTINCT pd.source_program, ip.depth + 1
        FROM impacted_programs ip
        JOIN program_dependencies pd ON pd.target_program = ip.program_path
        WHERE pd.source_program != ip.program_path
          AND ip.depth < 20
    )
    SELECT DISTINCT program_path, MIN(depth) as depth
    FROM impacted_programs
    WHERE program_path != ?
    GROUP BY program_path
    ORDER BY depth, program_path
    """
    return pd.read_sql_query(sql, conn, params=(program_path, program_path))


def upstream_dependencies(conn, program_path):
    """Find all programs and tables upstream of a given program.

    Reverse direction: find tables this program reads → programs that write those →
    tables those read → etc. Also follows program dependencies.
    """
    sql = """
    WITH RECURSIVE
    upstream(program_path, depth) AS (
        SELECT ?, 0

        UNION

        -- Programs that write tables read by an upstream program
        SELECT DISTINCT w.program_path, u.depth + 1
        FROM upstream u
        JOIN table_operations r ON r.program_path = u.program_path AND r.operation_type = 'read'
        JOIN table_operations w ON w.table_name = r.table_name AND w.operation_type = 'create'
        WHERE w.program_path != u.program_path
          AND u.depth < 20

        UNION

        -- Programs that this program includes/calls
        SELECT DISTINCT pd.target_program, u.depth + 1
        FROM upstream u
        JOIN program_dependencies pd ON pd.source_program = u.program_path
        WHERE pd.target_program != u.program_path
          AND u.depth < 20
    )
    SELECT DISTINCT program_path, MIN(depth) as depth
    FROM upstream
    WHERE program_path != ?
    GROUP BY program_path
    ORDER BY depth, program_path
    """
    return pd.read_sql_query(sql, conn, params=(program_path, program_path))


def table_impact(conn, table_name):
    """Find all programs that read or write a given table."""
    sql = """
    SELECT program_path, operation_type, database_type, source_line
    FROM table_operations
    WHERE table_name = ?
    ORDER BY operation_type, program_path
    """
    return pd.read_sql_query(sql, conn, params=(table_name,))


def credential_report(conn):
    """Return all programs that have credential findings."""
    sql = """
    SELECT program_path, credential_findings
    FROM programs
    WHERE credential_findings IS NOT NULL AND credential_findings != '[]'
    ORDER BY program_path
    """
    return pd.read_sql_query(sql, conn)
