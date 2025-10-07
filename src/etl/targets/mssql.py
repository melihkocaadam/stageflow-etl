
from __future__ import annotations
import os
from typing import List
import pandas as pd
from ..utils.db import mssql_conn
from ..utils.pandas_map import infer_sql_types

STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "stg")

class MsSqlStagingTarget:
    def __init__(self, source_alias: str, target_table: str):
        self.source_alias = self._sanitize(source_alias)
        self.target_table = self._sanitize(target_table)
        self.full_name = f"{STAGING_SCHEMA}.[{self.source_alias}_{self.target_table}]"
        self.cn = mssql_conn()

    @staticmethod
    def _sanitize(name: str) -> str:
        import re
        n = re.sub(r"[^A-Za-z0-9_]", "_", name)
        return n.upper()

    def ensure_table(self, df: pd.DataFrame):
        cur = self.cn.cursor()
        cur.execute(
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{STAGING_SCHEMA}') EXEC('CREATE SCHEMA {STAGING_SCHEMA}');"
        )
        types = infer_sql_types(df)
        cols_sql = ", ".join([f"[{c}] {t}" for c, t in types.items()])
        cols_sql += ", [_etl_loaded_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()"
        cur.execute(
            f"""
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='{self.source_alias}_{self.target_table}' AND schema_id=SCHEMA_ID('{STAGING_SCHEMA}'))
            CREATE TABLE {self.full_name} ({cols_sql});
            """
        )
        self.cn.commit()

    def upsert(self, df: pd.DataFrame, key_columns: List[str]) -> int:
        if df.empty:
            return 0
        tmp = f"##tmp_{self.source_alias}_{self.target_table}"
        cur = self.cn.cursor()

        types = infer_sql_types(df)
        cur.execute(f"CREATE TABLE {tmp} (" + ", ".join([f"[{c}] {t}" for c, t in types.items()]) + ");")

        placeholders = ",".join(["?"] * len(df.columns))
        insert_sql = f"INSERT INTO {tmp}([" + "],[".join(df.columns) + "]) VALUES ({placeholders})"
        cur.fast_executemany = True
        cur.executemany(insert_sql, df.where(pd.notnull(df), None).values.tolist())

        on_expr = " AND ".join([f"t.[{k}] = s.[{k}]" for k in key_columns])
        set_expr = ", ".join([f"[{c}] = s.[{c}]" for c in df.columns if c not in key_columns])
        columns = ", ".join([f"[{c}]" for c in df.columns])
        select_cols = ", ".join([f"s.[{c}]" for c in df.columns])
        merge_sql = f"""
        MERGE {self.full_name} AS t
        USING {tmp} AS s
          ON {on_expr}
        WHEN MATCHED THEN UPDATE SET {set_expr}
        WHEN NOT MATCHED BY TARGET THEN INSERT ({columns}) VALUES ({select_cols});
        DROP TABLE {tmp};
        """
        cur.execute(merge_sql)
        affected = cur.rowcount
        self.cn.commit()
        return affected if affected is not None else 0

    def close(self):
        try:
            self.cn.close()
        except Exception:
            pass
