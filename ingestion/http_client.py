"""Simple JSON HTTP client helpers."""

from __future__ import annotations

import json
import os
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


class HttpClientError(RuntimeError):
    """Raised when HTTP requests fail."""


def _env_float(name: str, default: float) -> float:
    """Read float environment variable with fallback."""

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_int(name: str, default: int) -> int:
    """Read integer environment variable with fallback."""

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def _retry_sleep(attempt: int, backoff_s: float) -> None:
    """Sleep with exponential backoff based on retry attempt index."""

    time.sleep(backoff_s * (2**attempt))


def _is_retryable_http_error(exc: HTTPError) -> bool:
    """Return True when HTTP status is generally transient."""

    return exc.code == 429 or exc.code >= 500


def get_json(
    url: str,
    params: dict[str, Any] | None = None,
    timeout_s: float | None = None,
    max_retries: int | None = None,
    retry_backoff_s: float | None = None,
) -> Any:
    """Fetch and decode JSON from an HTTP GET endpoint.

    Args:
        url: Base URL.
        params: Optional query params.
        timeout_s: Socket timeout in seconds.
        max_retries: Number of retries after first request failure.
        retry_backoff_s: Base backoff in seconds used between retries.

    Returns:
        Parsed JSON payload.

    Raises:
        HttpClientError: If request fails or payload is invalid JSON.
    """

    query = urlencode(params or {})
    request_url = f"{url}?{query}" if query else url
    timeout_value = timeout_s if timeout_s is not None else _env_float("L2_HTTP_TIMEOUT_S", 15.0)
    retries = max_retries if max_retries is not None else _env_int("L2_HTTP_MAX_RETRIES", 3)
    backoff = retry_backoff_s if retry_backoff_s is not None else _env_float("L2_HTTP_RETRY_BACKOFF_S", 1.0)

    for attempt in range(retries + 1):
        try:
            with urlopen(request_url, timeout=timeout_value) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw)
        except HTTPError as exc:
            if _is_retryable_http_error(exc) and attempt < retries:
                _retry_sleep(attempt=attempt, backoff_s=backoff)
                continue
            raise HttpClientError(f"HTTP error {exc.code} for {request_url}") from exc
        except URLError as exc:
            if attempt < retries:
                _retry_sleep(attempt=attempt, backoff_s=backoff)
                continue
            raise HttpClientError(f"Connection error for {request_url}: {exc.reason}") from exc
        except TimeoutError as exc:
            if attempt < retries:
                _retry_sleep(attempt=attempt, backoff_s=backoff)
                continue
            raise HttpClientError(f"Connection timeout for {request_url}") from exc
        except json.JSONDecodeError as exc:
            raise HttpClientError(f"Invalid JSON from {request_url}") from exc

    raise HttpClientError(f"Connection error for {request_url}: max retries exceeded")
