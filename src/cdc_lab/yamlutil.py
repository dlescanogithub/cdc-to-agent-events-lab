"""Tiny flat YAML reader for scenario files. Maps of scalars only, no nesting."""

from __future__ import annotations


def parse_flat_yaml(text: str) -> dict[str, object]:
    """Parse `key: value` lines. Comments start with `#`. Nested blocks error."""
    data: dict[str, object] = {}
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line[0] in {" ", "\t"} or line.startswith("-"):
            raise ValueError(
                f"line {lineno}: only flat key: value mappings are supported"
            )
        if ":" not in line:
            raise ValueError(f"line {lineno}: expected key: value")
        key, raw_value = line.split(":", 1)
        name = key.strip()
        if not name:
            raise ValueError(f"line {lineno}: empty key")
        if name in data:
            raise ValueError(f"line {lineno}: duplicate key {name}")
        data[name] = _scalar(raw_value.strip())
    return data


def _scalar(value: str) -> object:
    if value in {"", "null", "~"}:
        return None
    if value == "true":
        return True
    if value == "false":
        return False
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value
