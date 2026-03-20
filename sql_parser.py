"""
Lightweight SQL parsing utilities for BqForge.
Uses sqlparse for reliable comment stripping and normalization,
then focused regex for pattern extraction.
"""

import re
import sqlparse
from sqlparse import tokens as T


def strip_comments(sql: str) -> str:
    """Remove -- single-line and /* */ block comments from SQL."""
    result = []
    for statement in sqlparse.parse(sql):
        for token in statement.flatten():
            if token.ttype in (T.Comment.Single, T.Comment.Multiline):
                result.append(" ")
            else:
                result.append(token.value)
    return "".join(result)


def clean(sql: str) -> str:
    """Strip comments and collapse whitespace. Returns normalized SQL string."""
    s = strip_comments(sql)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_table_refs(sql: str) -> list[str]:
    """
    Extract table references from FROM and JOIN clauses.
    Handles backtick-quoted names and up to 3-part references (project.dataset.table).
    Returns deduplicated list in order of appearance.
    """
    s = clean(sql)
    # Match FROM/JOIN keyword followed by a table reference
    # Table ref: optional backtick wrapping, 1-3 dot-separated parts
    pattern = (
        r"(?:FROM|JOIN)\s+"
        r"`?([a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+){0,2})`?"
        r"(?:\s+(?:AS\s+)?\w+)?"
    )
    matches = re.findall(pattern, s, re.IGNORECASE)

    # Skip SQL keywords that can appear after FROM/JOIN in edge cases
    _skip = {
        "select", "with", "where", "having", "on", "unnest",
        "lateral", "each", "join", "inner", "left", "right",
        "outer", "cross", "full",
    }
    tables = [m for m in matches if m.lower().split(".")[0] not in _skip]
    return list(dict.fromkeys(tables))  # deduplicate, preserve order


def has_token(sql: str, *tokens: str) -> bool:
    """
    Return True if any of the given tokens appear in the cleaned, lowercased SQL.
    Use this instead of plain `in` on raw SQL to avoid matching inside comments.
    """
    s = clean(sql).lower()
    return any(t in s for t in tokens)


def count_keyword(sql: str, keyword: str) -> int:
    """Count occurrences of a keyword in comment-stripped, normalized SQL."""
    s = clean(sql).lower()
    # Use word boundary matching to avoid partial matches
    return len(re.findall(rf"\b{re.escape(keyword)}\b", s))


def get_where_clause(sql: str) -> str:
    """Return the portion of cleaned SQL from the last WHERE keyword onwards."""
    s = clean(sql).lower()
    idx = s.rfind(" where ")
    return s[idx:] if idx != -1 else ""
