"""Small state and fingerprint helpers for incremental lake transforms."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

FileFingerprint = dict[str, str | int]


def file_fingerprint(path: Path) -> FileFingerprint:
    """Return a stable content fingerprint for one input artifact."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size_bytes": stat.st_size,
        "sha256": digest.hexdigest(),
    }


def file_fingerprints(paths: list[Path]) -> dict[str, FileFingerprint]:
    """Return content fingerprints keyed by absolute path."""

    return {str(path.resolve()): file_fingerprint(path) for path in sorted(paths)}


def load_json_state(path: Path) -> dict[str, Any]:
    """Load a JSON state file, returning an empty state when absent or invalid."""

    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_state(path: Path, payload: dict[str, Any]) -> None:
    """Atomically write a JSON state payload."""

    path.parent.mkdir(parents=True, exist_ok=True)
    enriched = {**payload, "updated_at_utc": datetime.now(UTC).isoformat()}
    staging_path = path.with_name(f".{path.name}.staging")
    staging_path.write_text(json.dumps(enriched, indent=2, sort_keys=True), encoding="utf-8")
    staging_path.replace(path)
