"""Pure helpers for gap-fill time calculations."""

from __future__ import annotations

from datetime import UTC, datetime


def _last_closed_open_ms(interval_ms: int, now_utc: datetime | None = None) -> int:
    """Return open timestamp (ms) of latest fully closed candle."""

    now = now_utc or datetime.now(UTC)
    now_ms = int(now.timestamp() * 1000)
    return ((now_ms // interval_ms) - 1) * interval_ms


def _missing_ranges_ms(
    existing_open_times: list[datetime],
    interval_ms: int,
    end_open_ms: int,
) -> list[tuple[int, int]]:
    """Build contiguous missing open-time ranges from known candles to end-open timestamp."""

    existing_ms = sorted(
        {
            int(item.timestamp() * 1000)
            for item in existing_open_times
            if item.tzinfo is not None and int(item.timestamp() * 1000) <= end_open_ms
        }
    )
    if not existing_ms:
        return []

    ranges: list[tuple[int, int]] = []
    for previous, current in zip(existing_ms, existing_ms[1:], strict=False):
        gap_start_ms = previous + interval_ms
        gap_end_ms = current - interval_ms
        if gap_start_ms <= gap_end_ms:
            ranges.append((gap_start_ms, gap_end_ms))

    last_existing_ms = existing_ms[-1]
    if last_existing_ms + interval_ms <= end_open_ms:
        ranges.append((last_existing_ms + interval_ms, end_open_ms))

    return ranges
