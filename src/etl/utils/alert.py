
from __future__ import annotations
import os, json, requests

def send_slack(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return
    payload = {"text": text}
    headers = {"Content-Type": "application/json"}
    try:
        requests.post(url, data=json.dumps(payload), headers=headers, timeout=10)
    except Exception:
        pass

def send_teams(text: str):
    url = os.getenv("TEAMS_WEBHOOK_URL")
    if not url:
        return
    payload = {"text": text}
    headers = {"Content-Type": "application/json"}
    try:
        requests.post(url, data=json.dumps(payload), headers=headers, timeout=10)
    except Exception:
        pass
