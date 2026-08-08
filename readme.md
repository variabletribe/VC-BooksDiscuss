# BooksDiscuss VC Tracker Bot

A Telegram bot for the **@BooksDiscuss** group that tracks who actually shows up to voice/video
chats, turns attendance into a light gamification layer (XP, levels, streaks, badges), and
includes a full Rose-style moderation toolkit (warnings, bans, mutes, blocklist, filters) so the
group doesn't need a second bot for moderation.

---

## Purpose

Telegram's Bot API does **not** expose who joined a voice/video chat or how long they stayed —
only a handful of invite-related hints and the call's total duration. This bot works around that
limitation two ways:

1. **A Telethon "assistant" user account** (not a bot) polls the group call's live participant
   list every few seconds via Telegram's MTProto API, which *does* expose this data to a normal
   user account. This gives accurate per-person join/leave times.
2. **A Bot API fallback path** using invite-event hints, for groups where the assistant isn't
   configured, or for the rare short call the assistant's poll loop misses entirely.

On top of that raw attendance data, the bot adds:

- **Present attendance** — +1 "present day" for anyone who stays past a configurable threshold
  in a call, with day-streaks.
- **XP & levels** — 1 XP per minute in a call, with a 10-level progression curve.
- **Badges** — Marathoner, Night Owl, Week Warrior, Iron Streak, Veteran (all repeatable, shown
  as ×N).
- **AI recaps** — a short, casual natural-language summary of each call via Groq/Llama 3.3.
- **Reports** — all-time / monthly / weekly leaderboards, posted automatically or on demand.
- **Moderation** — warnings, bans, mutes (permanent and temporary), a word blocklist, and
  keyword auto-reply filters — modeled on Rose bot's command set so admins don't need to relearn
  anything.
- **Admin tooling** — CSV data export, per-user lookup, an admin DM relay/broadcast system.

---

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Language | Python 3.14 | |
| Bot framework | [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) v21+ | Async, `Application`/`JobQueue` for scheduled reports, webhook *and* polling support |
| Live VC tracking | [Telethon](https://github.com/LonamiWebs/Telethon) (MTProto, user account) | Only a real user account can see a group call's live participant list — the Bot API can't |
| Database | MongoDB Atlas via `pymongo` | Flexible schema for evolving stats/badges/mod data; aggregation pipelines for leaderboards |
| AI recap | [Groq](https://groq.com) API, `llama-3.3-70b-versatile` | Fast, free-tier-friendly inference for a short post-call summary |
| HTTP | `httpx` | Async HTTP for Groq calls and a raw Bot API fallback path (works even if PTB's polling hits a `Conflict`) |
| Hosting | [Render](https://render.com) (Web Service) | Webhook mode on the free/paid web tier; a small `http.server` stub keeps the port-scan happy in polling mode; a keep-alive thread fights free-tier idle spin-down |
| Config | `python-dotenv` + environment variables | 12-factor style config, no secrets in code |

### Architecture

```
bot.py         Main process. Registers all /commands, handles Telegram webhook/polling,
               posts VC-end summaries, schedules monthly/weekly reports, runs the
               moderation + blocklist + filter logic.

assistant.py   Background thread (Telethon). Polls each tracked group's live call state
               every ~2s, builds accurate per-user join/leave times, and calls into
               bot.py's posting/badge/AI-recap logic when a call ends.

db.py          All MongoDB access: VC sessions, attendance/XP/streaks/badges, warnings,
               blocklist, filters, admin-relay mappings, chat settings.

state.py       In-memory state shared between bot.py and assistant.py within the same
               process: which groups are configured, live "hint" data from Bot API
               service messages, and a claim mechanism so a VC-end event is never
               recorded twice (assistant path vs. Bot API fallback path racing).
```

Two independent code paths can observe "a VC ended" — the Telethon assistant's own poll loop,
and a Bot API fallback that exists as a safety net. `state.try_claim_vc_finalize()` gives
first-come-first-served ownership so the database is never written twice for the same call.

---

## Setup

### Required environment variables

| Variable | Purpose |
|---|---|
| `BOT_TOKEN` | From @BotFather |
| `MONGODB_URI` | MongoDB Atlas connection string |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | From [my.telegram.org](https://my.telegram.org) — for the Telethon assistant |
| `TELEGRAM_SESSION_STRING` | Generated locally by `session_login.py`, logged in as a normal **user** account (not a bot) |
| `ASSISTANT_GROUP_IDS` | Comma-separated supergroup chat ids the assistant should track, e.g. `-100111,-100222` |

### Optional

| Variable | Default | Purpose |
|---|---|---|
| `MONGODB_DB_NAME` | `vc_bot` | Mongo database name |
| `GROQ_API_KEY` | unset | Enables the AI recap; silently skipped if unset |
| `PRESENT_MIN_SECONDS` | `1200` (20 min) | Minutes in a call to count as a "present day" |
| `MONTHLY_REPORT_HOUR_UTC` | `9` | Hour monthly/weekly reports post |
| `ASSISTANT_POLL_SECONDS` | `2` | How often the assistant polls live call state |
| `ASSISTANT_FALLBACK_WAIT_SECONDS` | `8` | Grace period before the Bot API fallback takes over a VC-end event |
| `ADMIN_RELAY_CHAT_ID` | unset | Private group where DMs to the bot get relayed |
| `ADMIN_USER_IDS` | unset | Comma-separated user ids allowed to use `/message`, `/broadcast`, `/exportdata`, `/user`, and admin-relay replies |
| `BROADCAST_CHAT_ID` | first `ASSISTANT_GROUP_IDS` entry | Which group `/broadcast`, `/exportdata`, `/user` default to |
| `RENDER_EXTERNAL_URL` / `WEBHOOK_URL`, `PORT` | — | Enables webhook mode on Render; falls back to polling locally |
| `FORCE_POLLING` | unset | Force polling even on Render (useful if webhooks are unreliable on the free tier) |
| `KEEP_ALIVE_DISABLE` | unset | Disables the free-tier keep-alive pinger |

### Local run

```bash
pip install -r requirements.txt
python session_login.py        # one-time: generates TELEGRAM_SESSION_STRING
python bot.py
```

---

## Commands

### Stats & reports

| Command | Purpose |
|---|---|
| `/vcreport` | All-time leaderboard: VCs joined + total hours, per user |
| `/attendance` | Present-day leaderboard (people who crossed the attendance threshold) |
| `/monthreport` | Previous calendar month's leaderboard |
| `/weekly` | Last 7 days: top hours + streak leaders |

### Personal progress

| Command | Purpose |
|---|---|
| `/mystats` | Your full profile — attendance, VCs, hours, join dates, streak, XP, level |
| `/level` | Your XP and level |
| `/xpleaderboard` | Top 10 XP earners in the group |
| `/streak` | Your current and longest attendance streak |
| `/badges` | Your earned badges |

### Utility

| Command | Purpose |
|---|---|
| `/vcstatus` | This group's chat id + whether the assistant is actively tracking it |

### Moderation — everyone can use

| Command | Usage | Purpose |
|---|---|---|
| `/warns` | `/warns [reply / user_id / @username]` | Check a user's warnings; defaults to your own if no target given |
| `/blocklist` | `/blocklist` | View the current blocklisted words and mode |
| `/filters` | `/filters` | List saved filter keywords |

### Moderation — admins only

| Command | Usage | Purpose |
|---|---|---|
| `/warn` | `/warn [reply / id / @user] [reason]` | Add a warning; auto-applies the configured punishment once the limit is hit, then resets the count |
| `/resetwarn` | `/resetwarn [reply / id / @user]` | Clear a user's warnings |
| `/warnlimit` | `/warnlimit [n]` | View or set warns-before-punishment (default 3). Viewing needs no admin rights; setting does |
| `/warnmode` | `/warnmode [ban / mute / kick]` | View or set what happens at the warn limit (default `ban`) |
| `/ban` | `/ban [reply / id / @user] [reason]` | Permanent ban |
| `/tban` | `/tban [reply / id / @user] <time> [reason]` | Temporary ban — `<time>` is `30m`, `2h`, `1d`, `1w`. Enforced natively by Telegram (`until_date`), so it survives bot restarts |
| `/unban` | `/unban [id / @user]` | Lift a ban early |
| `/kick` | `/kick [reply / id / @user] [reason]` | Remove from the group (they can rejoin — this is ban+immediate-unban) |
| `/mute` | `/mute [reply / id / @user] [reason]` | Restrict indefinitely |
| `/tmute` | `/tmute [reply / id / @user] <time> [reason]` | Temporary mute, same `<time>` syntax as `/tban`, also enforced natively by Telegram |
| `/unmute` | `/unmute [id / @user]` | Lift a mute early, restoring the group's normal permissions |
| `/addblocklist` | `/addblocklist word1 word2 ...` | Add words to the blocklist |
| `/unblocklist` | `/unblocklist word1 word2 ...` | Remove words from the blocklist |
| `/blocklistmode` | `/blocklistmode [delete / warn / mute / kick / ban]` | What happens when a blocklisted word is posted — the message is always deleted; this sets what (if anything) also happens to the sender |
| `/filter` | `/filter <keyword> <reply text>` | Save an auto-reply: whenever `<keyword>` appears in a message, the bot replies with the saved text |
| `/stop` | `/stop <keyword>` | Remove a saved filter |

All moderation actions refuse to target group owners/admins (including anonymous admins posting
as the group), so a mis-set blocklist word or a mistaken command can never lock the mods out of
their own group.

### Admin only (VC-stats management)

| Command | Usage | Purpose |
|---|---|---|
| `/streakboard` | `/streakboard` | Every member's current + best streak, ranked |
| `/reports` | `/reports on\|off` | Toggle the automatic monthly report for this group |
| `/removeuser` | `/removeuser USER_ID` | Wipe a user's VC stats and attendance |
| `/finduser` | `/finduser NAME` | Look up a user's numeric id by name or an old @username |
| `/message` | `/message USER_ID text` | DM any known user directly, by id |
| `/broadcast` | `/broadcast text` (or reply to a message with `/broadcast`) | Message everyone who has ever joined a tracked VC |
| `/exportdata` | `/exportdata [chat_id]` — **DM only** | CSV of every user who's joined a VC: hours, present days, streaks, XP, level, join dates |
| `/user` | `/user USER_ID [chat_id]` — **DM only** | Full stats for any one user by id, without needing them to run `/mystats` themselves |

`/exportdata` and `/user` are restricted to a private chat with the bot and to user ids listed in
`ADMIN_USER_IDS` — they're cross-group admin tools, not group-scoped commands, so they don't fit
the per-group admin model the rest of the moderation commands use.

---

## Messages the bot sends automatically

These are not triggered by a `/command` — they fire on their own:

- **VC ended** (up to 4 messages, in order): call summary → present attendance → new badges
  (if any) → AI recap (if `GROQ_API_KEY` is set).
- **Monthly report** — 1st of the month, `MONTHLY_REPORT_HOUR_UTC`, to every group with
  `/reports` enabled.
- **Weekly digest** — every Monday at `MONTHLY_REPORT_HOUR_UTC`, if the group had any VC
  activity that week.
- **DM relay** — any private message to the bot is copied into `ADMIN_RELAY_CHAT_ID`; an admin's
  reply to that copy is delivered back to the original sender.

---

## Known limitations

- Group join dates are only tracked from whenever this feature shipped — there's no historical
  data for members who joined earlier (`/mystats` shows "Unknown" for them, honestly, not as a
  bug).
- If the group has "Hide join/leave messages" enabled, the bot can't see new-member join events
  at all.
- The Bot API fallback path's per-person time estimates are rough — they're derived from invite
  hints, not a real participant list, and are only used when the Telethon assistant is
  unavailable or misses a short call.
- The AI recap depends on Groq's API being reachable; it's skipped silently (not an error) if the
  request fails or `GROQ_API_KEY` isn't set.
