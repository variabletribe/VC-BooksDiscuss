# BooksDiscuss VC Tracker Bot

A Telegram bot for the **@BooksDiscuss** group that tracks who actually shows up to voice/video
chats, turns attendance into a light gamification layer (XP, levels, streaks, badges), manages
VC topic suggestions with permanent serial numbers, and includes a full moderation toolkit
(warnings, bans, mutes, blocklist, filters, link-locking, captcha, flood control) — so the group
doesn't need a second bot for moderation.

---

## Purpose

Telegram's Bot API does **not** expose who joined a voice/video chat or how long they stayed —
only a handful of invite-related hints and the call's total duration. This bot works around that
two ways:

1. **A Telethon "assistant" user account** (not a bot) polls the group call's live participant
   list every few seconds via Telegram's MTProto API, which *does* expose this data to a normal
   user account. This gives accurate per-person join/leave times.
2. **A Bot API fallback path** using invite-event hints, for groups where the assistant isn't
   configured, or for the rare short call the assistant's poll loop misses entirely.

On top of that raw attendance data, the bot adds:

- **Present attendance** — +1 "present day" for anyone who stays past a configurable threshold
  in a call, with day-streaks.
- **XP & levels** — 1 XP per minute in a call (plus small XP grants for engagement, like
  upvoting a VC topic), with a 10-level progression curve.
- **Badges** — Marathoner, Night Owl, Week Warrior, Iron Streak, Veteran (all repeatable, shown
  as ×N).
- **AI recaps** — a short, casual natural-language summary of each call via Groq/Llama 3.3.
- **Reports** — all-time / monthly / weekly leaderboards, posted automatically or on demand.
- **VC Topic Management** — members suggest discussion topics; each gets a permanent, never-reused
  serial number, can be upvoted, and moves through Active → Done/Deleted states.
- **Moderation** — warnings, bans, mutes (permanent and temporary), a word blocklist, keyword
  auto-reply filters (text or any media type), link-locking, new-member captcha verification,
  flood control, a searchable moderation log, and one-tap admin paging.
- **Admin tooling** — CSV data export, per-user lookup with full warning history, a live health check, and an admin DM relay/broadcast system.
- **Warning History** — **permanent, never-deleted** warning records. `/resetwarn` only clears the *active* count; old warnings remain viewable via `/mywarns` (users) and `/user` (bot admins).

---

## How it works (architecture)

```
bot.py         Main process. Registers all /commands, handles Telegram webhook/polling,
               posts VC-end summaries, schedules monthly/weekly reports, and runs the
               moderation, blocklist, filter, link-lock, captcha, flood-control, and
               topic logic.

assistant.py   Background thread (Telethon). Polls each tracked group's live call state
               every ~2s, builds accurate per-user join/leave times, and calls into
               bot.py's posting/badge/AI-recap logic when a call ends.

db.py          All MongoDB access: VC sessions, attendance/XP/streaks/badges, warnings,
               blocklist, filters, VC topics, moderation log, known-user (username)
               memory, admin-relay mappings, chat settings.

state.py       In-memory state shared between bot.py and assistant.py within the same
               process: which groups are configured, live "hint" data from Bot API
               service messages, and a claim mechanism so a VC-end event is never
               recorded twice (assistant path vs. Bot API fallback path racing).
```

Two independent code paths can observe "a VC ended" — the Telethon assistant's own poll loop,
and a Bot API fallback that exists as a safety net. `state.try_claim_vc_finalize()` gives
first-come-first-served ownership so the database is never written twice for the same call.

### Tech stack

| Layer | Choice | Why |
|---|---|---|
| Language | Python 3.14 | |
| Bot framework | [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) v21+ | Async, `Application`/`JobQueue` for scheduled reports and timers, inline-button callbacks, webhook *and* polling support |
| Live VC tracking | [Telethon](https://github.com/LonamiWebs/Telethon) (MTProto, user account) | Only a real user account can see a group call's live participant list — the Bot API can't |
| Database | MongoDB Atlas via `pymongo` | Flexible schema for evolving stats/badges/mod/topic data; atomic `findOneAndUpdate` for permanent topic serials and race-safe voting |
| AI recap | [Groq](https://groq.com) API, `llama-3.3-70b-versatile` | Fast, free-tier-friendly inference for a short post-call summary |
| HTTP | `httpx` | Async HTTP for Groq calls, `/health` checks, and a raw Bot API fallback path |
| Hosting | [Render](https://render.com) (Web Service) | Webhook mode on the free/paid web tier; keep-alive thread fights free-tier idle spin-down |
| Config | `python-dotenv` + environment variables | 12-factor style config, no secrets in code |

---

## Who can do what — the admin model, explained

This bot has **two completely separate admin systems**. Mixing them up is the most common source
of confusion, so here's exactly how each one works and why.

### 1. Group admin (most moderation commands)

These commands check the group's **real Telegram admin/owner status**, live, every time —
nothing is stored by the bot. If Telegram says you're an admin of the group, you can use them.

**How to become one:** Group Info → Administrators → Add Admin. No bot config, no redeploy —
it takes effect immediately, the next time that person runs a command.

This covers moderation (`/warn`, `/ban`, `/mute`, `/blocklist`, `/lock`, `/captcha`, `/setflood`,
etc.), VC topic management (`/topicdone`, `/deletetopic`), and group-scoped admin tools
(`/streakboard`, `/reports`, `/removeuser`, `/finduser`).

Anonymous admins ("send as group") are recognized correctly too — the bot detects this case
specifically so a real admin posting anonymously isn't mistaken for a regular member.

Every action that targets a person (`/warn`, `/ban`, `/mute`, etc.) refuses to run against
another group owner/admin, so a mis-set blocklist word or a mistaken command can never lock the
mods out of their own group.

### 2. Bot admin (`ADMIN_USER_IDS`)

A small, separate list of Telegram **user IDs** (not group-specific) set as an environment
variable on Render. This covers commands that aren't scoped to a single group —
`/message`, `/broadcast`, `/exportdata`, `/user`, `/health`.

**Why separate:** `/exportdata` and `/user` take a `chat_id` argument and can pull data from
*any* tracked group, and `/broadcast`/`/message` can reach users across groups — none of that
fits the "admin of this one group" model the rest of the bot uses. Being a group admin in
Telegram does **not** unlock these; being listed in `ADMIN_USER_IDS` does.

**How to add someone:**
1. Get their numeric Telegram user ID (have them message [@userinfobot](https://t.me/userinfobot), or use `/finduser` in a group they're in).
2. On Render → your service → **Environment** → edit `ADMIN_USER_IDS`, comma-separating IDs:
   `123456789,987654321`.
3. Save — Render redeploys automatically. This one **does** require a redeploy, since it's an
   env var, not stored in the database.

### 3. Anyone (no admin status needed)

Stats, personal progress, VC topic browsing/adding/voting, and the timer are open to every group member. Users can also see their **own full warning history** with `/mywarns` — active and cleared warnings, with dates, reasons, and who issued them.

---

## The `/start` menu

Running `/start` no longer dumps a giant wall of text. Instead it shows six category buttons:

**📊 Stats & Progress · 🗂️ VC Topics · 🛡️ Moderation · 🔔 Everyone · 🛠️ Group Admin Tools · 👑 Bot Owner Tools**

Tapping a category shows that category's commands as buttons; tapping a command shows exactly
what it does, its usage syntax, and who can use it — with a button to go back to the category or
jump to the main menu. Nothing is auto-deleted, so it can be left open and browsed at leisure.

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
| `GROQ_API_KEY` | unset | Enables the AI recap and `/health`'s Groq check; silently skipped if unset |
| `PRESENT_MIN_SECONDS` | `1200` (20 min) | Minutes in a call to count as a "present day" |
| `MONTHLY_REPORT_HOUR_UTC` | `9` | Hour monthly/weekly reports post |
| `ASSISTANT_POLL_SECONDS` | `2` | How often the assistant polls live call state |
| `ASSISTANT_FALLBACK_WAIT_SECONDS` | `8` | Grace period before the Bot API fallback takes over a VC-end event |
| `ADMIN_RELAY_CHAT_ID` | unset | Private group where DMs to the bot get relayed |
| `ADMIN_USER_IDS` | unset | Comma-separated user ids — see "Bot admin" above |
| `BROADCAST_CHAT_ID` | first `ASSISTANT_GROUP_IDS` entry | Which group `/broadcast`, `/exportdata`, `/user` default to |
| `RENDER_EXTERNAL_URL` / `WEBHOOK_URL`, `PORT` | — | Enables webhook mode on Render; falls back to polling locally |
| `FORCE_POLLING` | unset | Force polling even on Render |
| `KEEP_ALIVE_DISABLE` | unset | Disables the free-tier keep-alive pinger |

### Local run

```bash
pip install -r requirements.txt
python session_login.py        # one-time: generates TELEGRAM_SESSION_STRING
python bot.py
```

### Bot permissions

For moderation, blocklist, link-locking, captcha, and filters to actually work, the bot must be
a group **admin** with, at minimum: **Delete messages**, **Ban users**, and **Restrict members**.
Without these, affected commands reply with an error explaining what to check, rather than
failing silently.

---

## Commands

Full reference below, grouped exactly as the `/start` menu groups them. This table is generated
directly from the same data structure the bot's interactive menu uses (`HELP_COMMANDS` in
`bot.py`), so it can't silently drift out of sync with what the bot actually does.

### 📊 Stats & Progress

| Command | Usage | Description | Who can use it |
|---|---|---|---|
| `/vcreport` | `/vcreport` | All-time leaderboard: VCs joined and total hours, per user. | Anyone |
| `/attendance` | `/attendance` | Present-day leaderboard — people who crossed the attendance threshold. | Anyone |
| `/monthreport` | `/monthreport` | Previous calendar month's leaderboard. | Anyone |
| `/weekly` | `/weekly` | Last 7 days: top hours and streak leaders. | Anyone |
| `/vcstatus` | `/vcstatus` | This group's chat id, and whether the Telethon assistant is actively tracking it. | Anyone |
| `/mystats` | `/mystats` | Your full profile: attendance, VCs, hours, join dates, streak, XP, level. | Anyone |
| `/level` | `/level` | Your XP and level. | Anyone |
| `/xpleaderboard` | `/xpleaderboard` | Top 10 XP earners in the group. | Anyone |
| `/streak` | `/streak` | Your current and longest attendance streak. | Anyone |
| `/badges` | `/badges` | Your earned badges. | Anyone |

### 🗂️ VC Topics

Every topic gets a permanent serial number, assigned atomically so it's never duplicated or
reused — even after the topic is deleted.

| Command | Usage | Description | Who can use it |
|---|---|---|---|
| `/addtopic` | `/addtopic <topic text>` | Suggest a discussion topic. Gets a permanent serial number that's never reused, even after deletion. Blocks exact duplicates against active topics. | Anyone |
| `/topics` | `/topics` | Active topics only, ranked by votes (see `/upvote`) so the group's real priority shows. | Anyone |
| `/alltopics` | `/alltopics` | Every topic ever added, sorted by serial. Done shows ✅, deleted shows struck through. | Anyone |
| `/deletedtopics` | `/deletedtopics` | Deleted topics, with their original serial numbers. | Anyone |
| `/upvote` | `/upvote <serial>` | Support an active topic so it ranks higher in `/topics`. One vote per person per topic; +2 XP the first time. | Anyone |
| `/topicdone` | `/topicdone <serial>` | Mark an active topic as Done. | Group admin |
| `/deletetopic` | `/deletetopic <serial>` | Delete a topic. The serial number is never reused for a future topic. | Group admin |

### 🛡️ Moderation

Targets can be given as a **reply** to their message, or as their **user id** / **@username**
(username resolution uses the bot's own memory, built passively from messages it's seen in the
group, since Telegram doesn't let bots look up arbitrary members by username on demand).

| Command | Usage | Description | Who can use it |
|---|---|---|---|
| `/warn` | `/warn [reply / id / @user] [reason]` | Warn a user. At the warn limit (default 3), auto-applies the configured punishment (default: kick) and resets the count. | Group admin |
| `/warns` | `/warns [reply / id / @user]` | Check a user's **active** warnings (counts toward the limit). Defaults to your own if no target is given. | Group admin |
| `/resetwarn` | `/resetwarn [reply / id / @user]` | Clear a user's **active** warnings back to zero. **Warning history is never deleted** – cleared warnings are marked as "Cleared" and remain visible via `/mywarns` and `/user`. | Group admin |
| `/mywarns` | `/mywarns` | See your **own full warning history** – active and cleared, with dates, reasons, and who issued them. | Anyone |
| `/warnlimit` | `/warnlimit [n]` | View, or set, how many warnings trigger auto-punishment (default 3). | Group admin |
| `/warnmode` | `/warnmode [ban/mute/kick]` | View, or set, what happens at the warn limit (default kick). | Group admin |
| `/ban` | `/ban [reply / id / @user] [reason]` | Permanently ban a user. Shows a confirm/cancel button first — nothing happens until you tap Confirm. | Group admin |
| `/tban` | `/tban [reply / id / @user] <time> [reason]` | Temporary ban — time as `30m`, `2h`, `1d`, or `1w`. Telegram itself lifts it automatically, even across a bot restart. | Group admin |
| `/unban` | `/unban [id / @user]` | Lift a ban early. | Group admin |
| `/kick` | `/kick [reply / id / @user] [reason]` | Remove someone from the group — they CAN rejoin (this is a ban immediately followed by an unban). | Group admin |
| `/mute` | `/mute [reply / id / @user] [reason]` | Restrict a user from sending anything, indefinitely. | Group admin |
| `/tmute` | `/tmute [reply / id / @user] <time> [reason]` | Temporary mute — same time syntax as `/tban`, also enforced natively by Telegram. | Group admin |
| `/unmute` | `/unmute [reply / id / @user]` | Lift a mute early, restoring the group's normal permissions. | Group admin |
| `/blocklist` | `/blocklist` | View the current blocklisted words and the action mode. | Group admin |
| `/addblocklist` | `/addblocklist word1 word2 ...` | Add one or more words to the blocklist. | Group admin |
| `/unblocklist` | `/unblocklist word1 word2 ...` | Remove words from the blocklist. | Group admin |
| `/blocklistmode` | `/blocklistmode [delete/warn/mute/kick/ban]` | View, or set, what happens when someone posts a blocklisted word. The message is always deleted; this controls what (if anything) also happens to the sender. | Group admin |
| `/filter` | `/filter <keyword> <reply text>` — or reply to any message with `/filter <keyword>` | Save an auto-reply for a keyword. Typed text keeps its exact formatting; replying to a message saves it verbatim (photo, video, sticker, document, etc.). | Group admin |
| `/filters` | `/filters` | List every saved filter keyword. | Group admin |
| `/stop` | `/stop <keyword>` | Remove a saved filter. | Group admin |
| `/lock` | `/lock links` | From now on, delete any non-admin message containing a link. **Now correctly detects anonymous admins.** | Group admin || `/unlock` | `/unlock links` | Turn link-locking back off. | Group admin |
| `/locks` | `/locks` | View which content types are currently locked. | Group admin |
| `/captcha` | `/captcha on\|off` | New-member verification: joiners are muted and must tap a button within 5 minutes, or they're auto-kicked (and can rejoin to retry). | Anyone can view; group admin to change |
| `/setflood` | `/setflood <count> [window_seconds]` or `/setflood off` | Auto-punish anyone posting too many messages too fast. Off by default. | Anyone can view; group admin to change |
| `/floodmode` | `/floodmode [mute/kick/ban]` | What happens when flood control triggers. | Anyone can view; group admin to change |
| `/modlog` | `/modlog [count]` | The last moderation actions in this group: who did what, to whom, when, and why. | Group admin |

Blocklist mode behavior specifically:
- **`delete`** — message removed, nothing else happens.
- **`warn`** — message removed, plus a plain notice that auto-deletes after 30s. This does
  **not** touch the formal `/warn` system or count toward `/warnlimit`.
- **`mute` / `kick` / `ban`** — message removed and the sender immediately punished.

### 🔔 Everyone

| Command | Usage | Description | Who can use it |
|---|---|---|---|
| `/timer` | `/timer <N>m` | One-shot reminder timer — minutes only, max 20m, one running per group at a time. | Anyone |
| `/canceltimer` | `/canceltimer` | Cancel the currently running timer early. | Anyone |
| `/mywarns` | `/mywarns` | See your own full warning history – active and cleared, with dates and reasons. | Anyone |

Also: writing **`@admin`** anywhere in a group message pings every current admin with a clickable mention (works even for admins without a public username). The anonymous admin placeholder is never pinged. Rate-limited to once per 60 seconds per group.

### 🛠️ Group Admin Tools

| Command | Usage | Description | Who can use it |
|---|---|---|---|
| `/streakboard` | `/streakboard` | Every member's current and best attendance streak, ranked. | Group admin |
| `/reports` | `/reports on\|off` | Toggle the automatic monthly report for this group. | Group admin |
| `/removeuser` | `/removeuser USER_ID` | Wipe a user's VC stats and attendance. Shows a preview and asks for confirmation first. | Group admin |
| `/finduser` | `/finduser NAME` | Look up a user's numeric id by name or an old @username. | Group admin |

### 👑 Bot Owner Tools

These are the only commands gated by `ADMIN_USER_IDS` rather than group admin status — see
"Who can do what" above for why.

| Command | Usage | Description | Who can use it |
|---|---|---|---|
| `/message` | `/message USER_ID text` | DM any known user directly, by their numeric id. | Bot admin |
| `/broadcast` | `/broadcast text` (or reply to a message with `/broadcast`) | Message everyone who has ever joined a tracked VC. Shows the audience size and asks for confirmation before sending. | Bot admin |
| `/exportdata` | `/exportdata [chat_id]` | CSV of every user who's joined a VC — hours, present days, streaks, XP, level, join dates. | Bot admin, DM only |
| `/user` | `/user USER_ID [chat_id]` | Full stats for any one user by id, **including full warning history** (active + cleared). DM only. | Bot admin, DM only |
| `/health` | `/health` | Checks MongoDB, the Telegram Bot API, the Telethon assistant, and Groq — catches a silent failure before it's noticed the hard way. | Bot admin, DM only |

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
- **Blocklist / link-lock / captcha notices** — short messages posted when enforcement
  triggers; most self-delete after 15–30 seconds so they don't clutter the chat.
- **Timer expiry** — the `/timer` announcement when its duration elapses.

---

## Known limitations

- Group join dates are only tracked from whenever this feature shipped — there's no historical
  data for members who joined earlier (`/mystats` shows "Unknown" for them, honestly, not as a
  bug).
- If the group has "Hide join/leave messages" enabled, the bot can't see new-member join events
  at all (and captcha can't trigger for them either).
- The Bot API fallback path's per-person time estimates are rough — derived from invite hints,
  not a real participant list, and only used when the Telethon assistant is unavailable or
  misses a short call.
- The AI recap depends on Groq's API being reachable; it's skipped silently (not an error) if the
  request fails or `GROQ_API_KEY` isn't set.
- `@username` resolution for moderation commands relies on the bot having seen at least one
  message from that person since this feature was deployed. Until then, reply to their message
  instead of typing `@username`.
- List commands (`/streakboard`, `/topics`, `/alltopics`, `/modlog`, etc.) cap out at a bounded
  number of rows and show "+N more not shown" beyond that, to stay under Telegram's 4096-character
  message limit in very large or very active groups.
- Confirmation prompts (`/ban`, `/removeuser`, `/broadcast`) and timers/flood tracking are held
  in memory, not the database — they expire after a short window (2 minutes for confirmations)
  or reset on a bot restart. This is an intentional tradeoff: these are short-lived by nature, so
  persisting them isn't worth the overhead.

  ## Recent changes

### 🔐 Permanent Warning History
- Warnings are **never deleted** – `/resetwarn` now only marks warnings as "Cleared" instead of deleting them.
- Every warning ever issued (reason, date, who by) stays queryable forever.
- Users can see their **full history** (active + cleared) with `/mywarns`.
- Bot owners can see any user's full history via `/user`.

### 🛡️ Anonymous Admin Detection
- Link-lock and `@admin` tagging now correctly detect admins posting anonymously ("Send as group").
- Anonymous admin messages are **not** deleted by link-lock.
- The `GroupAnonymousBot` placeholder is never pinged in `@admin` tags.

### 🐛 Bug Fixes
- **`/resetwarn` truly resets** – after a reset, warnings accumulate from zero again, and the user won't be prematurely banned.
