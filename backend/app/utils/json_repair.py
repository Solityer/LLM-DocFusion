"""JSON repair utilities for handling unreliable LLM output."""
import re
import json
from typing import Any, Optional


def extract_json_from_text(text: str) -> Optional[str]:
    """Extract JSON from LLM output that may contain extra text."""
    if not text or not text.strip():
        return None

    text = text.strip()

    # Remove markdown code fences
    text = re.sub(r'^```(?:json)?\s*\n?', '', text, flags=re.MULTILINE)
    text = re.sub(r'\n?```\s*$', '', text, flags=re.MULTILINE)
    text = text.strip()

    # Try direct parse
    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass

    # Find JSON object or array in text
    for pattern in [
        r'(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})',  # nested objects
        r'(\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\])',  # nested arrays
    ]:
        matches = re.findall(pattern, text, re.DOTALL)
        for m in reversed(matches):  # prefer last match (more likely the answer)
            try:
                json.loads(m)
                return m
            except json.JSONDecodeError:
                continue

    # Greedy approach: find first { or [ and last } or ]
    first_brace = -1
    last_brace = -1
    for i, c in enumerate(text):
        if c in '{[':
            if first_brace == -1:
                first_brace = i
            break

    for i in range(len(text) - 1, -1, -1):
        if text[i] in '}]':
            last_brace = i
            break

    if first_brace >= 0 and last_brace > first_brace:
        candidate = text[first_brace:last_brace + 1]
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            repaired = repair_json(candidate)
            if repaired:
                return repaired

    return None


def repair_json(text: str) -> Optional[str]:
    """Attempt to repair broken JSON."""
    if not text:
        return None

    text = text.strip()

    # Fix common issues
    # 1. Trailing commas
    text = re.sub(r',\s*([}\]])', r'\1', text)

    # 2. Single quotes to double quotes (careful with apostrophes)
    # Only replace quotes that appear to be string delimiters
    text = re.sub(r"(?<![\\])\'", '"', text)

    # 3. Missing quotes around keys
    text = re.sub(r'(\{|,)\s*([a-zA-Z_\u4e00-\u9fa5][a-zA-Z0-9_\u4e00-\u9fa5]*)\s*:', r'\1"\2":', text)

    # 4. Unescaped newlines inside strings
    # This is hard to fix perfectly, just try
    
    # 5. Fix None/True/False -> null/true/false
    text = re.sub(r'\bNone\b', 'null', text)
    text = re.sub(r'\bTrue\b', 'true', text)
    text = re.sub(r'\bFalse\b', 'false', text)

    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass

    # Try truncation repair: if JSON is cut off, close open brackets
    brackets = []
    in_string = False
    escape = False
    for ch in text:
        if escape:
            escape = False
            continue
        if ch == '\\':
            escape = True
            continue
        if ch == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch in '{[':
            brackets.append(ch)
        elif ch == '}' and brackets and brackets[-1] == '{':
            brackets.pop()
        elif ch == ']' and brackets and brackets[-1] == '[':
            brackets.pop()

    # Close unclosed brackets
    if brackets:
        closing = ''
        for b in reversed(brackets):
            closing += '}' if b == '{' else ']'
        # Remove incomplete last entry (trailing comma or partial key/value)
        repaired = re.sub(r',\s*"[^"]*"?\s*:?\s*$', '', text)
        repaired = re.sub(r',\s*$', '', repaired)
        repaired = repaired + closing
        try:
            json.loads(repaired)
            return repaired
        except json.JSONDecodeError:
            pass

    return None


def safe_parse_json(text: str) -> tuple[Optional[Any], str]:
    """Parse JSON from potentially messy LLM output.
    Returns (parsed_value, error_message). error_message is empty on success.
    """
    if not text or not text.strip():
        return None, "Empty input"

    extracted = extract_json_from_text(text)
    if extracted:
        try:
            return json.loads(extracted), ""
        except json.JSONDecodeError as e:
            return None, f"JSON parse error after extraction: {e}"

    repaired = repair_json(text)
    if repaired:
        try:
            return json.loads(repaired), ""
        except json.JSONDecodeError as e:
            return None, f"JSON parse error after repair: {e}"

    return None, f"Could not extract valid JSON from: {text[:200]}"
