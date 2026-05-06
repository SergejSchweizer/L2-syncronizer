"""Tests for HTTP client configuration behavior."""

from __future__ import annotations

import pytest

from ingestion import http_client
from ingestion.config import Config


def test_default_http_request_config_loads_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify default HTTP settings are cached for a process."""

    calls = 0

    def fake_load_config() -> Config:
        nonlocal calls
        calls += 1
        return {
            "http": {
                "timeout_s": 8,
                "max_retries": 2,
                "retry_backoff_s": 1,
            }
        }

    http_client.default_http_request_config.cache_clear()
    monkeypatch.setattr(http_client, "load_config", fake_load_config)

    try:
        first = http_client.default_http_request_config()
        second = http_client.default_http_request_config()

        assert first == second
        assert first.timeout_s == 8
        assert first.max_retries == 2
        assert first.retry_backoff_s == 1
        assert calls == 1
    finally:
        http_client.default_http_request_config.cache_clear()
