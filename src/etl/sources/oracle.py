
from __future__ import annotations
from typing import Optional
import pandas as pd
from ..utils.db import oracle_conn
from .base import BaseSource

class OracleSource(BaseSource):
    def fetch(self, cfg: dict, last_value: Optional[str]) -> pd.DataFrame:
        base_sql: str = cfg["source_sql"]
        inc_col: Optional[str] = cfg.get("increment_column")
        sql = base_sql
        params = None
        if inc_col and last_value:
            sql = f"SELECT * FROM ({base_sql}) WHERE {inc_col} > :lastval"
            params = {"lastval": last_value}
        with oracle_conn() as cn:
            df = pd.read_sql(sql, cn, params=params)
        for c in df.columns:
            if any(k in c.upper() for k in ("DATE", "TIME")):
                try:
                    df[c] = pd.to_datetime(df[c])
                except Exception:
                    pass
        return df
