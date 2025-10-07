
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd

class BaseSource(ABC):
    @abstractmethod
    def fetch(self, cfg: dict, last_value: Optional[str]) -> pd.DataFrame:
        raise NotImplementedError

    def get_new_increment(self, df: pd.DataFrame, increment_column: Optional[str]) -> Optional[str]:
        if increment_column and not df.empty and increment_column in df.columns:
            try:
                return str(df[increment_column].max())
            except Exception:
                return None
        return None
