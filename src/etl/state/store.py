
from __future__ import annotations
import os
from typing import Optional

ETL_SCHEMA = os.getenv("ETL_SCHEMA", "etl")

def ensure_log_tables(cn):
    cur = cn.cursor()
    cur.execute(f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{ETL_SCHEMA}')
      EXEC('CREATE SCHEMA {ETL_SCHEMA}');
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_runs' AND schema_id = SCHEMA_ID('{ETL_SCHEMA}'))
      CREATE TABLE {ETL_SCHEMA}.etl_runs(
        run_id INT IDENTITY(1,1) PRIMARY KEY,
        table_name NVARCHAR(256),
        start_time DATETIME2,
        end_time DATETIME2 NULL,
        status NVARCHAR(20),
        rows_read INT DEFAULT 0,
        rows_loaded INT DEFAULT 0,
        increment_last_value NVARCHAR(200) NULL,
        error_text NVARCHAR(MAX) NULL
      );
    """)
    cn.commit()

def get_last_increment(cn, table_name: str) -> Optional[str]:
    cur = cn.cursor()
    cur.execute(
        f"""
        SELECT TOP(1) increment_last_value
        FROM {ETL_SCHEMA}.etl_runs
        WHERE table_name = ? AND status='SUCCESS'
        ORDER BY run_id DESC
        """,
        (table_name,),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def log_run_start(cn, table_name: str) -> int:
    cur = cn.cursor()
    cur.execute(
        f"""
        INSERT INTO {ETL_SCHEMA}.etl_runs(table_name, start_time, status)
        VALUES (?, SYSUTCDATETIME(), 'RUNNING');
        SELECT SCOPE_IDENTITY();
        """ ,
        (table_name,),
    )
    run_id = int(cur.fetchone()[0])
    cn.commit()
    return run_id

def log_run_end(
    cn,
    run_id: int,
    status: str,
    rows_read: int,
    rows_loaded: int,
    inc_last: Optional[str],
    error: Optional[str],
):
    cur = cn.cursor()
    cur.execute(
        f"""
        UPDATE {ETL_SCHEMA}.etl_runs
        SET end_time = SYSUTCDATETIME(),
            status = ?,
            rows_read = ?,
            rows_loaded = ?,
            increment_last_value = ?,
            error_text = ?
        WHERE run_id = ?
        """,
        (status, rows_read, rows_loaded, inc_last, error, run_id),
    )
    cn.commit()
