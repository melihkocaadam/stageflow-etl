
from __future__ import annotations
import os
from typing import Optional
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from .base import BaseSource

class GoogleSheetsSource(BaseSource):
    def fetch(self, cfg: dict, last_value: Optional[str]) -> pd.DataFrame:
        sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        scopes = [os.getenv("GOOGLE_SHEETS_SCOPES", "https://www.googleapis.com/auth/spreadsheets.readonly")]
        creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
        gc = gspread.authorize(creds)

        sh = gc.open_by_key(cfg["spreadsheet_id"])
        ws = sh.worksheet(cfg.get("worksheet", "Sheet1"))
        rows = ws.get_all_records()
        return pd.DataFrame(rows)
