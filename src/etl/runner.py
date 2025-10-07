
from __future__ import annotations
import os
import pandas as pd
from .config import load_config, TableConfig
from .utils.logging import get_logger
from .utils.db import mssql_conn
from .state.store import ensure_log_tables, get_last_increment, log_run_start, log_run_end
from .targets.mssql import MsSqlStagingTarget
from .sources.base import BaseSource
from .sources.oracle import OracleSource
from .sources.webapi import WebApiSource
from .sources.google_sheets import GoogleSheetsSource
from .sources.onedrive_excel import OneDriveExcelSource
from .transformers.builtin import build_transform
from .utils.alert import send_slack, send_teams
from .utils.retry import retry_call

logger = get_logger("etl.runner")
CONFIG_PATH = os.getenv("ETL_CONFIG", "config/tables.json")

def make_source(kind: str) -> BaseSource:
    if kind == "oracle":
        return OracleSource()
    if kind == "webapi":
        return WebApiSource()
    if kind == "google_sheets":
        return GoogleSheetsSource()
    if kind == "onedrive_excel":
        return OneDriveExcelSource()
    raise ValueError(f"Unknown source_type: {kind}")

def process_table(cfg: TableConfig):
    table_key = f"{cfg.source_alias}.{cfg.table_name}"
    logger.info(f"Running table: {table_key} -> {cfg.target_table}")
    cn = mssql_conn()
    run_id = None
    try:
        ensure_log_tables(cn)
        run_id = log_run_start(cn, table_key)

        last_val = get_last_increment(cn, table_key) if cfg.increment_column else None
        src = make_source(cfg.source_type)
        df = retry_call(src.fetch, cfg.__dict__, last_val, retries=3)
        rows_read = len(df)

        transforms = (getattr(cfg, "transforms", None) or cfg.__dict__.get("transforms")) or []
        for op in transforms:
            tr = build_transform(op)
            df = tr.apply(df)

        target = MsSqlStagingTarget(cfg.source_alias, cfg.target_table)
        if rows_read == 0:
            df = pd.DataFrame(columns=cfg.key_columns)
        target.ensure_table(df)

        loaded = target.upsert(df, cfg.key_columns) if rows_read > 0 else 0
        new_inc = src.get_new_increment(df, cfg.increment_column)

        log_run_end(cn, run_id, "SUCCESS", rows_read, loaded, new_inc, None)
        target.close()
        logger.info(f"OK: rows_read={rows_read} loaded={loaded} new_inc={new_inc}")
    except Exception as e:
        logger.exception("FAILED")
        try:
            if run_id is not None:
                log_run_end(cn, run_id, "FAILED", 0, 0, None, str(e))
        except Exception:
            pass
        msg = f"ETL FAILED: {table_key} — {e}"
        send_slack(msg)
        send_teams(msg)
        raise
    finally:
        try:
            cn.close()
        except Exception:
            pass

def main():
    app_cfg = load_config(CONFIG_PATH)
    for t in app_cfg.tables:
        retry_call(process_table, t, retries=2)

if __name__ == "__main__":
    main()
