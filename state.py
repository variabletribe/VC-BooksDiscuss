"""Shared flags between bot.py and assistant.py."""

assistant_running: bool = False
# Supergroup IDs (negative) where the assistant tracks VC; bot skips Bot-API VC summaries for these.
assistant_chat_ids: set[int] = set()
