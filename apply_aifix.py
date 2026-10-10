#!/usr/bin/env python3
"""
apply_aifix.py - fixes the AI voice-chat recap (Groq retired the model the bot used).

Run from the folder that contains bot.py:

    python apply_aifix.py --dry-run   # only check that every edit point is found
    python apply_aifix.py             # patch bot.py

What it changes: the recap now uses openai/gpt-oss-120b (falling back to gpt-oss-20b),
the model can be overridden with GROQ_MODEL on Render, failures are logged with Groq's
own error message, and /health tells you if the recap model has disappeared again.
Same safety as the other scripts: nothing changes if an edit point is missing, the result
is syntax-checked, bot.py.bak keeps the original, and running it twice is harmless.
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

MARKER_BOT = "# --- Groq (AI recap) ---"

GROQ_HELPER = r'''# --- Groq (AI recap) --------------------------------------------------------
# Groq retires models regularly (llama-3.3-70b-versatile was shut down on 16 Aug 2026), so
# the model is configurable (GROQ_MODEL on Render) and the bot falls back to the next one
# in this list if a model no longer exists. gpt-oss models "think" first, and those tokens
# count against the limit, so they get a low reasoning effort and a roomy token budget.

_GROQ_MODEL_FALLBACKS = ["openai/gpt-oss-120b", "openai/gpt-oss-20b"]


def _groq_models() -> list[str]:
    custom = (os.environ.get("GROQ_MODEL") or "").strip()
    models: list[str] = []
    for m in ([custom] if custom else []) + _GROQ_MODEL_FALLBACKS:
        if m not in models:
            models.append(m)
    return models


async def _groq_recap_text(api_key: str, prompt: str) -> str | None:
    """Ask Groq for the recap, trying each model in turn. Every failure is logged with
    the model name and Groq's own error message, so Render's logs show what went wrong."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        for model in _groq_models():
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_completion_tokens": 1024,
                "temperature": 0.8,
            }
            if model.startswith("openai/gpt-oss"):
                payload["reasoning_effort"] = "low"
            try:
                r = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json=payload,
                )
            except Exception:
                logger.exception("Groq summary request failed (model=%s)", model)
                continue
            if r.status_code != 200:
                logger.warning("Groq summary failed: model=%s HTTP %s %s", model, r.status_code, r.text[:300])
                if r.status_code in (401, 403):
                    return None  # bad/revoked key: other models won't help
                continue
            try:
                choice = r.json()["choices"][0]
                text = (choice["message"].get("content") or "").strip()
            except (KeyError, IndexError, ValueError, AttributeError):
                logger.warning("Groq summary: unexpected response shape from %s: %s", model, r.text[:300])
                continue
            if text:
                return text
            logger.warning("Groq summary: %s returned no text (finish_reason=%s)", model, choice.get("finish_reason"))
    return None


'''

BOT_OPS = [
    ("sub", "bot: Groq helper with model fallback",
     r'''async def generate_ai_vc_summary(''',
     GROQ_HELPER + "async def generate_ai_vc_summary("),
    ("sub", "bot: recap request uses the helper",
     r'''    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.8,
                },
            )
        if r.status_code != 200:
            logger.warning("Groq summary failed: HTTP %s %s", r.status_code, r.text[:300])
            return None
        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        return text or None
    except Exception:
        logger.exception("Groq summary request failed")
        return None''',
     r'''    return await _groq_recap_text(api_key, prompt)'''),
    ("sub", "bot: /health checks that the recap model exists",
     r'''            lines.append("✅ Groq: reachable" if r.status_code == 200 else f"❌ Groq: HTTP {r.status_code}")''',
     r'''            if r.status_code != 200:
                lines.append(f"❌ Groq: HTTP {r.status_code}")
            else:
                try:
                    available = {m.get("id") for m in r.json().get("data", [])}
                except Exception:
                    available = set()
                usable = [m for m in _groq_models() if m in available]
                if usable:
                    lines.append(f"✅ Groq: reachable, recap model {usable[0]} is available")
                elif available:
                    lines.append("❌ Groq: key works but none of the recap models exist any more. Set GROQ_MODEL on Render."
                                 )
                else:
                    lines.append("✅ Groq: reachable")'''),
]

# ----------------------------------------------------------------------------
# Patch engine
# ----------------------------------------------------------------------------


def anchor_regex(anchor: str) -> re.Pattern:
    """Exact text, except trailing spaces on each line and whitespace-only lines may differ."""
    lines = anchor.split("\n")
    pieces = [re.escape(l.rstrip()) if l.strip() else "" for l in lines]
    return re.compile("[ \\t]*\n".join(pieces))


def _func_span(text: str, name: str) -> tuple[int, int] | None:
    tree = ast.parse(text)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            start = min([node.lineno] + [d.lineno for d in node.decorator_list])
            return start, node.end_lineno
    return None


def apply_ops(text: str, ops: list, label: str) -> tuple[str | None, list[str]]:
    """Returns (new_text, problems). new_text is None if any problem was found."""
    problems: list[str] = []
    original = text

    # Pass 1: every edit point must exist exactly once in the ORIGINAL file.
    for op in ops:
        kind, name = op[0], op[1]
        if kind == "sub":
            n = len(list(anchor_regex(op[2]).finditer(original)))
            if n != 1:
                problems.append(f"{label}: '{name}' - edit point found {n} times (expected 1)")
        elif kind == "delete_line":
            n = sum(1 for l in original.split("\n") if op[2] in l)
            if n != 1:
                problems.append(f"{label}: '{name}' - line found {n} times (expected 1)")
        elif kind == "remove_func":
            if _func_span(original, op[2]) is None:
                problems.append(f"{label}: '{name}' - function {op[2]} not found")
    if problems:
        return None, problems

    # Pass 2: apply in order.
    for op in ops:
        kind, name = op[0], op[1]
        if kind == "sub":
            pat, new = anchor_regex(op[2]), op[3]
            matches = list(pat.finditer(text))
            if len(matches) != 1:
                return None, [f"{label}: '{name}' - edit point changed during patching ({len(matches)} matches)"]
            m = matches[0]
            text = text[: m.start()] + new + text[m.end():]
        elif kind == "append":
            text = text.rstrip("\n") + "\n" + op[2]
        elif kind == "delete_line":
            lines = text.split("\n")
            lines = [l for l in lines if op[2] not in l]
            text = "\n".join(lines)
        elif kind == "remove_func":
            span = _func_span(text, op[2])
            if span is None:
                return None, [f"{label}: '{name}' - function vanished during patching"]
            start, end = span
            lines = text.split("\n")
            while end < len(lines) and not lines[end].strip():
                end += 1  # also swallow the blank lines after it
            text = "\n".join(lines[: start - 1] + lines[end:])
    try:
        compile(text, label, "exec")
    except SyntaxError as exc:
        return None, [f"{label}: patched code has a syntax error at line {exc.lineno}: {exc.msg}"]
    return text, []


def patch_file(path: Path, ops: list, marker: str, dry: bool) -> bool:
    raw = path.read_bytes().decode("utf-8")
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    if marker in text:
        print(f"- {path.name}: already patched, skipping")
        return True
    new_text, problems = apply_ops(text, ops, path.name)
    if new_text is None:
        print(f"X {path.name}: NOT patched")
        for p in problems:
            print("    " + p)
        return False
    if dry:
        print(f"OK {path.name}: all {len(ops)} edit points found (dry run, nothing written)")
        return True
    shutil.copy2(path, path.with_name(path.name + ".bak"))
    if crlf:
        new_text = new_text.replace("\n", "\r\n")
    path.write_bytes(new_text.encode("utf-8"))
    print(f"OK {path.name}: patched ({len(ops)} edits), original saved as {path.name}.bak")
    return True


def main() -> int:
    dry = "--dry-run" in sys.argv
    path = Path("bot.py")
    if not path.exists():
        print("Run this from the folder that contains bot.py")
        return 1
    text = path.read_bytes().decode("utf-8").replace("\r\n", "\n")
    if MARKER_BOT not in text:
        new_text, problems = apply_ops(text, BOT_OPS, "bot.py")
        if new_text is None:
            print("X bot.py: cannot be patched")
            for p in problems:
                print("    " + p)
            print("\nNothing was changed. Send me the lines above and I'll adjust the script.")
            return 1
    if not patch_file(path, BOT_OPS, MARKER_BOT, dry):
        return 1
    if not dry:
        print("\nDone. Commit and push bot.py and let Render redeploy. Then run /health in a private chat with the bot.")
    return 0


if __name__ == "__main__":
    sys.exit(main())