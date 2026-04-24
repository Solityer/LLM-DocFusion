"""Text processing utilities."""
import re
from difflib import SequenceMatcher
from typing import Optional


def similarity(s1: str, s2: str) -> float:
    """String similarity ratio."""
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, str(s1).strip(), str(s2).strip()).ratio()


def keyword_overlap(s1: str, s2: str) -> float:
    """Keyword overlap ratio between two strings."""
    k1 = set(re.findall(r'[\u4e00-\u9fa5a-zA-Z0-9]+', s1.lower()))
    k2 = set(re.findall(r'[\u4e00-\u9fa5a-zA-Z0-9]+', s2.lower()))
    if not k1 or not k2:
        return 0.0
    overlap = k1 & k2
    return len(overlap) / min(len(k1), len(k2))


def best_column_match(target: str, candidates: list[str], threshold: float = 0.45) -> Optional[str]:
    """Find the best matching column name from candidates."""
    best = None
    best_score = 0.0

    for c in candidates:
        # Exact match
        if target == c:
            return c

        # Substring containment
        if target in c or c in target:
            score = 0.85
        else:
            score = max(similarity(target, c), keyword_overlap(target, c))

        if score > best_score and score >= threshold:
            best_score = score
            best = c

    return best


def truncate_text(text: str, max_len: int = 4000) -> str:
    """Truncate text to max length."""
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len] + f"\n... (truncated, total {len(text)} chars)"


def extract_numbers(text: str) -> list[str]:
    """Extract numbers from text."""
    nums = re.findall(r'[\d,]+\.?\d*', text)
    return [n.replace(',', '') for n in nums if n.replace(',', '').replace('.', '').strip()]


def clean_cell_value(v) -> str:
    """Clean a cell value to string."""
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in ('nan', 'none', 'nat', ''):
        return ""
    return s
