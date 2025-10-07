
from __future__ import annotations
import pandas as pd

def infer_sql_types(df: pd.DataFrame) -> dict[str, str]:
    types: dict[str, str] = {}
    for col, dtype in df.dtypes.items():
        c = col.replace(" ", "_")
        if pd.api.types.is_datetime64_any_dtype(dtype):
            types[c] = "DATETIME2 NULL"
        elif pd.api.types.is_integer_dtype(dtype):
            types[c] = "BIGINT NULL"
        elif pd.api.types.is_float_dtype(dtype):
            types[c] = "DECIMAL(38,10) NULL"
        else:
            types[c] = "NVARCHAR(4000) NULL"
    return types
