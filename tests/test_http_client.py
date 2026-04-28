"""Tests for HTTP client retry behavior."""

from __future__ import annotations

from email.message import Message
from urllib.error import HTTPError, URLError

import pytest

from ingestion import http_client
from ingestion.http_client import HttpClientError, get_json


class _FakeResponse:
    """Context-manager response stub used for urlopen monkeypatching."""

    def __init__(self, payload: str) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload.encode("utf-8")

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb


def test_get_json_retries_on_url_error_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, int] = {"count": 0}

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del url, timeout
        calls["count"] += 1
        if calls["count"] == 1:
            raise URLError("timed out")
        return _FakeResponse('{"ok": true}')

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)

    payload = get_json("https://example.com", max_retries=2, retry_backoff_s=0.0)
    assert payload == {"ok": True}
    assert calls["count"] == 2


def test_get_json_retries_on_http_500_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, int] = {"count": 0}

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del timeout
        calls["count"] += 1
        if calls["count"] == 1:
            raise HTTPError(url=url, code=500, msg="server error", hdrs=Message(), fp=None)
        return _FakeResponse('{"ok": true}')

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)

    payload = get_json("https://example.com", max_retries=2, retry_backoff_s=0.0)
    assert payload == {"ok": True}
    assert calls["count"] == 2


def test_get_json_does_not_retry_on_http_400(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, int] = {"count": 0}

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        del timeout
        calls["count"] += 1
        raise HTTPError(url=url, code=400, msg="bad request", hdrs=Message(), fp=None)

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)

    with pytest.raises(HttpClientError, match="HTTP error 400"):
        get_json("https://example.com", max_retries=3, retry_backoff_s=0.0)
    assert calls["count"] == 1
