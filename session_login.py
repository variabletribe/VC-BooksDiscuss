"""
One-time: create TELEGRAM_SESSION_STRING for the assistant user account.

1. Get api_id / api_hash from https://my.telegram.org/apps
2. Run:  py session_login.py
3. Paste printed session string into Render env TELEGRAM_SESSION_STRING

The assistant account must be a normal user in each group you track (ASSISTANT_GROUP_IDS).
"""

import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()

api_id = int((os.environ.get("TELEGRAM_API_ID") or "0").strip() or "0")
api_hash = (os.environ.get("TELEGRAM_API_HASH") or "").strip()
if not api_id or not api_hash:
    raise SystemExit("Set TELEGRAM_API_ID and TELEGRAM_API_HASH (and optional .env)")

with TelegramClient(StringSession(), api_id, api_hash) as client:
    client.start()
    print("Add this to your environment as TELEGRAM_SESSION_STRING:\n")
    print(client.session.save())
