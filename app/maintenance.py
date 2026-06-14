from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class CleanupResult:
    removed_months: tuple[str, ...]
    freed_bytes: int


def _directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _latest_mtime(path: Path) -> float:
    mtimes = [path.stat().st_mtime]
    mtimes.extend(item.stat().st_mtime for item in path.rglob("*"))
    return max(mtimes)


def cleanup_cache(
    cache_dir: str | Path,
    *,
    protected_months: set[str],
    older_than_days: int,
) -> CleanupResult:
    root = Path(cache_dir)
    if not root.exists():
        return CleanupResult((), 0)
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    removed: list[str] = []
    freed_bytes = 0
    for directory in sorted(item for item in root.iterdir() if item.is_dir()):
        if directory.name in protected_months:
            continue
        modified = datetime.fromtimestamp(_latest_mtime(directory), timezone.utc)
        if modified >= cutoff:
            continue
        freed_bytes += _directory_size(directory)
        shutil.rmtree(directory)
        removed.append(directory.name)
    return CleanupResult(tuple(removed), freed_bytes)

