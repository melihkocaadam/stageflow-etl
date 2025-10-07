
from __future__ import annotations
from typing import Optional, List, Dict, Any
import requests
import pandas as pd
from ..utils.retry import retry_call
from .base import BaseSource

def _dig(obj: Any, path: Optional[str]):
    if not path:
        return obj
    cur = obj
    for k in path.split('.'):
        cur = cur[k]
    return cur

class WebApiSource(BaseSource):
    def fetch(self, cfg: dict, last_value: Optional[str]) -> pd.DataFrame:
        url: str = cfg["url"]
        method: str = cfg.get("method", "GET").upper()
        headers: Dict[str, str] = cfg.get("headers", {})
        base_params: Dict[str, Any] = dict(cfg.get("params", {}))
        json_path: Optional[str] = cfg.get("json_path")
        pagination: Dict[str, Any] = cfg.get("pagination", {})

        inc_col = cfg.get("increment_column")
        inc_param = cfg.get("increment_param", "since")
        if inc_col and last_value:
            base_params[inc_param] = last_value

        rows: List[Dict[str, Any]] = []

        if pagination and pagination.get("type") == "page":
            page = int(pagination.get("start_page", 1))
            max_pages = int(pagination.get("max_pages", 1))
            page_size_param = pagination.get("page_size_param")
            page_size = pagination.get("page_size")

            for _ in range(max_pages):
                params = dict(base_params)
                params[pagination.get("page_param", "page")] = page
                if page_size_param and page_size:
                    params[page_size_param] = page_size
                resp = retry_call(requests.request, method, url, headers=headers, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                items = _dig(data, json_path)
                if not items:
                    break
                rows.extend(items)
                page += 1
        else:
            resp = retry_call(requests.request, method, url, headers=headers, params=base_params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            items = _dig(data, json_path)
            rows = items if isinstance(items, list) else (items or [])

        return pd.DataFrame(rows)
