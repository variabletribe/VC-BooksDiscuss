"""Shared flags between bot.py and assistant.py."""

assistant_running: bool = False
# Supergroup IDs (negative) where the assistant tracks VC; bot skips Bot-API VC summaries for these.
assistant_chat_ids: set[int] = set()
# Set when assistant successfully posts a VC end summary (time.monotonic()); used for fallback dedupe.
assistant_vc_report_mono: dict[int, float] = {}
