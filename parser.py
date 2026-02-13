"""SAS parser — loads regex from patterns.yaml, applies to source text."""

import re
from pathlib import Path

import yaml


def load_patterns(yaml_path=None):
    """Load and compile all regex patterns from patterns.yaml.

    Returns dict: category_name → list of compiled pattern dicts.
    Each dict has: name, regex (compiled), groups (index→semantic name).
    """
    if yaml_path is None:
        yaml_path = Path(__file__).parent / "patterns.yaml"
    with open(yaml_path) as f:
        raw = yaml.safe_load(f)

    compiled = {}
    for category, entries in raw.items():
        compiled[category] = []
        for entry in entries:
            flags = 0
            for flag_name in entry.get("flags", []):
                flags |= getattr(re, flag_name)
            compiled[category].append({
                "name": entry["name"],
                "regex": re.compile(entry["pattern"], flags),
                "groups": {int(k): v for k, v in entry["groups"].items()},
            })
    return compiled


def _find_matches(source, patterns, category):
    """Apply all patterns in a category to source text, return list of match dicts."""
    results = []
    for pat in patterns[category]:
        for m in pat["regex"].finditer(source):
            line_num = source[:m.start()].count("\n") + 1
            match_dict = {"pattern_name": pat["name"], "line": line_num}
            for group_idx, semantic_name in pat["groups"].items():
                match_dict[semantic_name] = m.group(group_idx)
            results.append(match_dict)
    return results


def parse_table_writes(source, patterns):
    """Extract table write operations from SAS source."""
    return _find_matches(source, patterns, "table_write")


def parse_table_reads(source, patterns):
    """Extract table read operations from SAS source."""
    return _find_matches(source, patterns, "table_read")


def parse_includes(source, patterns):
    """Extract %INCLUDE directives from SAS source."""
    return _find_matches(source, patterns, "include")


def parse_macro_defs(source, patterns):
    """Extract %MACRO definitions from SAS source."""
    return _find_matches(source, patterns, "macro_def")


def parse_macro_calls(source, patterns):
    """Extract macro invocations from SAS source."""
    return _find_matches(source, patterns, "macro_call")


def parse_libnames(source, patterns):
    """Extract LIBNAME statements from SAS source."""
    return _find_matches(source, patterns, "libname")


def parse_let_statements(source, patterns):
    """Extract %LET variable assignments from SAS source."""
    results = _find_matches(source, patterns, "let_statement")
    for r in results:
        if "value" in r:
            r["value"] = r["value"].strip()
    return results


def parse_credentials(source, patterns):
    """Extract hardcoded credential patterns from SAS source."""
    return _find_matches(source, patterns, "credentials")
