"""
restore_backup.py - restore a database backup made by the bot (/backup or the scheduled one).

    python restore_backup.py vc_bot_backup_20261012_213000.json.gz
    python restore_backup.py FILE --only user_attendance vc_sessions   # just some collections

Needs MONGODB_URI (and optionally MONGODB_DB_NAME) in the environment or .env, exactly like
the bot. It UPSERTS by _id: documents in the backup overwrite the same documents in the
database, and nothing that is already in the database gets deleted.
"""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

import db as dbmod


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(description="Restore a vc_bot database backup.")
    ap.add_argument("file", help="the .json.gz file the bot sent you")
    ap.add_argument("--only", nargs="*", help="restore only these collections")
    ap.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = ap.parse_args()

    blob = Path(args.file).read_bytes()
    try:
        payload = dbmod.parse_backup(blob)
    except Exception as exc:
        print(f"Can't read that file: {exc}")
        return 1

    collections = payload.get("collections") or {}
    wanted = {k: v for k, v in collections.items() if not args.only or k in args.only}
    print(f"Backup made: {payload.get('created_at')}  (database: {payload.get('db')})")
    for name, docs in sorted(wanted.items()):
        print(f"  {name}: {len(docs)} documents")
    if not wanted:
        print("Nothing to restore (check the --only names).")
        return 1

    dbmod.init_db()
    if not args.yes:
        answer = input("\nType RESTORE to write these into the live database: ").strip()
        if answer != "RESTORE":
            print("Cancelled.")
            return 1
    done = dbmod.restore_backup(blob, only=args.only)
    for name, n in sorted(done.items()):
        print(f"restored {name}: {n}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
