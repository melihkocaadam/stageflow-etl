
from __future__ import annotations
import os, io
from typing import Optional
import requests
import pandas as pd
from msal import ConfidentialClientApplication
from .base import BaseSource

class OneDriveExcelSource(BaseSource):
    def _get_token(self) -> str:
        app = ConfidentialClientApplication(
            client_id=os.getenv("MSAL_CLIENT_ID"),
            client_credential=os.getenv("MSAL_CLIENT_SECRET"),
            authority=f"https://login.microsoftonline.com/{os.getenv('MSAL_TENANT_ID')}"
        )
        scopes = [os.getenv("MS_GRAPH_SCOPE", "https://graph.microsoft.com/.default")]
        result = app.acquire_token_silent(scopes, account=None)
        if not result:
            result = app.acquire_token_for_client(scopes=scopes)
        if "access_token" not in result:
            raise RuntimeError(f"MSAL token error: {result}")
        return result["access_token"]

    def fetch(self, cfg: dict, last_value: Optional[str]) -> pd.DataFrame:
        token = self._get_token()
        item_id = cfg["file_item_id"]
        worksheet = cfg.get("worksheet")
        url = f"https://graph.microsoft.com/v1.0/me/drive/items/{item_id}/content"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
        r.raise_for_status()
        content = io.BytesIO(r.content)
        if worksheet:
            df = pd.read_excel(content, sheet_name=worksheet)
        else:
            df = pd.read_excel(content)
        return df
