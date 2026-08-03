"""
One-off reset script: deletes ALL data for this bot from MongoDB Atlas.

This wipes:
  - vc_sessions           (every recorded VC call + per-user durations)
  - user_attendance       (present days, XP, levels, streaks, badges)
  - monthly_report_sent   (dedupe records for auto monthly reports)
  - chat_settings         (per-chat monthly-report on/off flag — optional, see below)

It does NOT delete the database or drop indexes; collections are left in place
(empty) so init_db()'s create_index calls keep working normally on next boot.

USAGE (run this once, locally or via Render's Shell tab, then stop):
  1. Make sure MONGODB_URI is set in your environment (same value as on Render).
     Locally: put it in your .env file, or export it:
       export MONGODB_URI="mongodb+srv://...."
  2. Run:
       python reset_database.py
  3. Confirm the prompt by typing: DELETE

By default chat_settings (your /reports on|off preference) is KEPT so you don't
have to reconfigure monthly reports. Pass --wipe-chat-settings to also clear it.
"""

from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


def main() -> None:
    parser = argparse.ArgumentParser(description="Wipe all VC bot data from MongoDB.")
    parser.add_argument(
        "--wipe-chat-settings",
        action="store_true",
        help="Also delete chat_settings (per-group monthly-report on/off preference).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive confirmation prompt (use with care).",
    )
    args = parser.parse_args()

    uri = os.environ.get("MONGODB_URI")
    if not uri:
        print("MONGODB_URI is not set. Set it in your environment or .env file and re-run.")
        sys.exit(1)

    db_name = os.environ.get("MONGODB_DB_NAME", "vc_bot")
    client = MongoClient(uri)
    db = client[db_name]

    collections_to_wipe = ["vc_sessions", "user_attendance", "monthly_report_sent"]
    if args.wipe_chat_settings:
        collections_to_wipe.append("chat_settings")

    print(f"Database: {db_name}")
    print("About to permanently delete ALL documents from:")
    for name in collections_to_wipe:
        count = db[name].count_documents({})
        print(f"  - {name}: {count} document(s)")
    if not args.wipe_chat_settings:
        kept_count = db["chat_settings"].count_documents({})
        print(f"  (keeping chat_settings: {kept_count} document(s) — pass --wipe-chat-settings to clear too)")

    if not args.yes:
        answer = input("\nType DELETE to confirm, anything else to cancel: ").strip()
        if answer != "DELETE":
            print("Cancelled. Nothing was deleted.")
            sys.exit(0)

    for name in collections_to_wipe:
        result = db[name].delete_many({})
        print(f"Deleted {result.deleted_count} document(s) from {name}")

    print("\nDone. The database is clean — all VC history, attendance, XP, levels, "
          "streaks and badges start fresh from today.")
    client.close()


if __name__ == "__main__":
    main()
