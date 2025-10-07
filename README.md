
# StageFlow ETL — Oracle/Web/Sheets/OneDrive ➜ MSSQL Staging (stg.DB_ADI_TABLO_ADI)

StageFlow ETL; Oracle, Web API, Google Sheets ve OneDrive/Excel kaynaklarından veri çekip
MSSQL Staging katmanına **stg.{SOURCE_ALIAS}_{TARGET_TABLE}** formatında **idempotent MERGE (UPSERT)** ile yükler.
Tüm kurallar **JSON konfig** ile yönetilir. OOP ve modüler tasarım.

## Özellikler
- Kaynaklar: **Oracle**, **Web API (pagination + incremental)**, **Google Sheets (service account)**, **OneDrive/Excel (Microsoft Graph)**
- Hedef: **MSSQL Staging** (isimlendirme: `stg.{SOURCE_ALIAS}_{TARGET_TABLE}`)
- **Transformers/**: kolon yeniden adlandırma, tip dönüşümü, kolon düşürme (JSON'dan zincir)
- **Retry & Backoff** + **Slack/Teams** alarmları
- **Log & Watermark**: `etl.etl_runs` tablosu

## Hızlı Başlangıç
```bash
python -m venv .venv
.venv\Scripts\activate   # Windows (PowerShell)
pip install -r requirements.txt

# .env'i doldurun (Oracle/MSSQL/Google/MS Graph/Slack/Teams)
# config/tables.json'u örnekten düzenleyin
python -m etl.runner
```

## İsimlendirme
- Staging tablo adı: **stg.{SOURCE_ALIAS}_{TARGET_TABLE}**
  - Örn: `stg.ORAIFS_CUSTOMER`, `stg.WEBSVC_NEWS`, `stg.GDRIVE_PRICE_LIST`, `stg.ONEDRIVE_SALES`

## Ortam Değişkenleri (.env)
- Oracle: `ORACLE_USER, ORACLE_PASS, ORACLE_DSN`
- MSSQL: `MSSQL_SERVER, MSSQL_DB, MSSQL_USER, MSSQL_PASS`
- Şemalar: `ETL_SCHEMA=etl`, `STAGING_SCHEMA=stg`
- Google Sheets: `GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEETS_SCOPES`
- Microsoft Graph: `MSAL_TENANT_ID, MSAL_CLIENT_ID, MSAL_CLIENT_SECRET, MS_GRAPH_SCOPE`
- Alerts: `SLACK_WEBHOOK_URL`, `TEAMS_WEBHOOK_URL`

## Lisans
MIT
