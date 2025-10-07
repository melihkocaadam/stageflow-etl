
from __future__ import annotations
from typing import Dict, List
import pandas as pd
from .base import BaseTransformer

class RenameColumns(BaseTransformer):
    def __init__(self, mapping: Dict[str, str]):
        self.mapping = mapping
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.mapping)

class CastColumns(BaseTransformer):
    def __init__(self, mapping: Dict[str, str]):
        self.mapping = mapping
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        for col, typ in self.mapping.items():
            if col not in df.columns:
                continue
            if typ == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif typ == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif typ == "str":
                df[col] = df[col].astype("string")
            elif typ == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

class DropColumns(BaseTransformer):
    def __init__(self, columns: List[str]):
        self.columns = columns
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=[c for c in self.columns if c in df.columns], errors="ignore")

def build_transform(op: dict) -> BaseTransformer:
    name = (op.get("name") or "").lower()
    args = op.get("args") or {}
    if name == "rename_columns":
        return RenameColumns(args.get("mapping", {}))
    if name == "cast_columns":
        return CastColumns(args.get("mapping", {}))
    if name == "drop_columns":
        return DropColumns(args.get("columns", []))
    raise ValueError(f"Unknown transformer: {name}")
