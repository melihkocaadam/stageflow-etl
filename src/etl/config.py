
from __future__ import annotations
import json
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class TableConfig:
    source_type: str
    source_alias: str
    table_name: str
    target_table: str
    key_columns: List[str]
    period_minutes: int
    increment_column: Optional[str] = None
    # generic fields for sources
    source_sql: Optional[str] = None
    url: Optional[str] = None
    method: Optional[str] = None
    headers: Optional[dict] = None
    params: Optional[dict] = None
    json_path: Optional[str] = None
    increment_param: Optional[str] = None
    spreadsheet_id: Optional[str] = None
    worksheet: Optional[str] = None
    file_item_id: Optional[str] = None
    transforms: Optional[list] = None

@dataclass
class AppConfig:
    default_period_minutes: int
    tables: List[TableConfig]

def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    tables = [TableConfig(**t) for t in raw["tables"]]
    return AppConfig(default_period_minutes=raw.get("default_period_minutes", 60), tables=tables)
