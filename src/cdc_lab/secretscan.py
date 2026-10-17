"""Look for committed credentials (local equivalent of a secret scanner).

Vendored utility, kept byte-identical across the portfolio repos.
Its sha256 is recorded in docs/PROVENANCE.md and checked by a test.
Patterns are specific on purpose, so the word "token" in prose is not a hit.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("xai_key", re.compile(r"(?<![A-Za-z0-9])xai-[A-Za-z0-9_-]{20,}")),
    ("anthropic_key", re.compile(r"(?<![A-Za-z0-9])sk-ant-[A-Za-z0-9_-]{20,}")),
    (
        "openai_key",
        re.compile(r"(?<![A-Za-z0-9])sk-(?:proj-|live-)?[A-Za-z0-9_-]{20,}"),
    ),
    ("elevenlabs_key", re.compile(r"(?<![A-Za-z0-9])sk_[A-Za-z0-9]{20,}")),
    ("github_token", re.compile(r"gh[pousr]_[A-Za-z0-9]{36}")),
    ("github_pat", re.compile(r"github_pat_[A-Za-z0-9_]{20,}")),
    ("aws_access_key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("slack_token", re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}")),
    ("bearer_token", re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]{20,}")),
    (
        "private_key",
        re.compile(
            r"-----BEGIN (?:RSA |OPENSSH |EC |DSA |ENCRYPTED )?PRIVATE KEY-----"
        ),
    ),
)

SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "node_modules",
    "var",
}


def scan_text(text: str) -> list[str]:
    """Return the names of the patterns that match `text`."""
    return [name for name, pattern in PATTERNS if pattern.search(text)]


def scan_repo(root: Path) -> list[str]:
    """Return `relative/path:pattern_name` for every hit under `root`."""
    findings: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.relative_to(root).parts):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for name in scan_text(text):
            findings.append(f"{path.relative_to(root)}:{name}")
    return findings


def main() -> int:
    findings = scan_repo(Path("."))
    if findings:
        for finding in findings:
            print(finding, file=sys.stderr)
        return 1
    print("secret scan: no findings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
