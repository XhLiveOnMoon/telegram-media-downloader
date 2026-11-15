from __future__ import annotations

import os
import re
from pathlib import Path

_INVALID = re.compile(r"[\\/:*?\"<>|\x00-\x1f]+")


def safe_filename(value: str, fallback: str = "telegram-media") -> str:
    name = _INVALID.sub("_", Path(value).name).strip(" .")
    if not name or name in {".", ".."}:
        name = fallback
    return name[:180]


def prepare_directories(complete: Path, incomplete: Path) -> None:
    complete.mkdir(parents=True, exist_ok=True)
    incomplete.mkdir(parents=True, exist_ok=True)
    complete.chmod(0o755)
    incomplete.chmod(0o755)


def reserve_incomplete_path(directory: Path, filename: str) -> Path:
    """Return a unique temporary path; never overwrite an existing download."""
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o755)
    name = safe_filename(filename)
    candidate = directory / f"{name}.part"
    index = 1
    while candidate.exists():
        candidate = directory / f"{Path(name).stem}-{index}{Path(name).suffix}.part"
        index += 1
    return candidate


def stable_partial_path(directory: Path, source_key: str, filename: str) -> Path:
    """Build a deterministic partial path so a later retry can resume it."""
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o755)
    return directory / f"{source_key}-{safe_filename(filename)}.part"
