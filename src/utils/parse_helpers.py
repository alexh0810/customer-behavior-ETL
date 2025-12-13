# src/utils/parse_helpers.py
"""Helpers for parsing LLM outputs and small utilities."""
import json
from typing import Optional


def extract_first_json_array(text: str) -> Optional[str]:
    """
    Find and return the substring corresponding to the first top-level JSON array
    in `text`. Returns None if not found. Respects quoted strings and escapes.
    """
    if not text:
        return None

    start = text.find("[")
    if start == -1:
        return None

    i = start
    depth = 0
    in_string = False
    escape = False

    while i < len(text):
        ch = text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
        i += 1

    return None


def parse_json_array_from_text(text: str):
    """Return parsed list from the first JSON array in text, or empty list."""
    if not text:
        return []
    text = text.strip()
    # try full parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass

    # extract substring
    arr_sub = extract_first_json_array(text)
    if arr_sub:
        try:
            return json.loads(arr_sub)
        except json.JSONDecodeError:
            return []

    # final fallback: try lines
    for ln in text.splitlines():
        ln = ln.strip()
        if ln.startswith("[") or ln.startswith("{"):
            try:
                parsed = json.loads(ln)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                continue
    return []
