from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from fitnick.integrations import bcounter


class DummyResponse:
    def __init__(self, status_code=200, payload=None, text=''):
        self.status_code = status_code
        self._payload = payload or {}
        self.ok = 200 <= status_code < 300
        self.text = text

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, existing=None, post_payload=None):
        self.existing = existing
        self.post_payload = post_payload or {
            'date': '08/29/2026',
            'time': '07:15:00 AM',
            'source': 'google_health',
        }
        self.get_calls = []
        self.post_calls = []

    def get(self, url, headers=None, timeout=0):
        self.get_calls.append((url, headers, timeout))
        return DummyResponse(payload={'today_date': '08/29/2026', 'wake_time_iso': self.existing})

    def post(self, url, headers=None, json=None, timeout=0):
        self.post_calls.append((url, headers, json, timeout))
        return DummyResponse(payload=self.post_payload)


def _set_sleep(monkeypatch, wake_time):
    monkeypatch.setattr(
        'fitnick.integrations.bcounter.get_latest_sleep_session',
        lambda: {'wake_time': wake_time, 'date': wake_time[:10]},
    )


def test_sync_pushes_todays_wake_time(monkeypatch):
    _set_sleep(monkeypatch, '2026-08-29T07:15:00-04:00')
    session = DummySession()

    result = bcounter.sync_latest_wake_time(
        base_url='https://bcounter.example/',
        api_key='secret',
        now=datetime(2026, 8, 29, 12, tzinfo=ZoneInfo('America/New_York')),
        session=session,
    )

    assert result['action'] == 'pushed'
    assert session.get_calls[0][0] == 'https://bcounter.example/wake-time'
    assert session.get_calls[0][1]['X-API-Key'] == 'secret'
    assert session.post_calls[0][2] == {
        'wake_time': '2026-08-29T07:15:00-04:00',
        'source': 'google_health',
    }


def test_sync_skips_exact_duplicate(monkeypatch):
    _set_sleep(monkeypatch, '2026-08-29T07:15:00-04:00')
    session = DummySession(existing='2026-08-29T07:15:00-04:00')

    result = bcounter.sync_latest_wake_time(
        base_url='https://bcounter.example',
        api_key='secret',
        now=datetime(2026, 8, 29, 12, tzinfo=ZoneInfo('America/New_York')),
        session=session,
    )

    assert result['reason'] == 'already_synced'
    assert session.post_calls == []


def test_sync_skips_previous_days_sleep(monkeypatch):
    _set_sleep(monkeypatch, '2026-08-28T07:15:00-04:00')
    session = DummySession()

    result = bcounter.sync_latest_wake_time(
        base_url='https://bcounter.example',
        api_key='secret',
        now=datetime(2026, 8, 29, 12, tzinfo=ZoneInfo('America/New_York')),
        session=session,
    )

    assert result['reason'] == 'wake_time_not_today'
    assert session.get_calls == []
    assert session.post_calls == []


def test_sync_interprets_naive_timestamp_in_configured_timezone(monkeypatch):
    _set_sleep(monkeypatch, '2026-08-29T07:15:00')
    session = DummySession()

    result = bcounter.sync_latest_wake_time(
        base_url='https://bcounter.example',
        api_key='secret',
        timezone_name='America/New_York',
        now=datetime(2026, 8, 29, 12, tzinfo=ZoneInfo('America/New_York')),
        session=session,
    )

    assert result['wake_time'] == '2026-08-29T07:15:00-04:00'


def test_sync_requires_bcounter_configuration(monkeypatch):
    monkeypatch.delenv('BCOUNTER_BASE_URL', raising=False)
    monkeypatch.delenv('BCOUNTER_API_KEY', raising=False)

    with pytest.raises(bcounter.BCounterConfigurationError) as exc:
        bcounter.sync_latest_wake_time()

    assert 'BCOUNTER_BASE_URL' in str(exc.value)
    assert 'BCOUNTER_API_KEY' in str(exc.value)


def test_sync_surfaces_bcounter_error(monkeypatch):
    _set_sleep(monkeypatch, '2026-08-29T07:15:00-04:00')

    class ErrorSession(DummySession):
        def get(self, url, headers=None, timeout=0):
            return DummyResponse(status_code=401, payload={'detail': 'Invalid API key'})

    with pytest.raises(bcounter.BCounterAPIError) as exc:
        bcounter.sync_latest_wake_time(
            base_url='https://bcounter.example',
            api_key='wrong',
            now=datetime(2026, 8, 29, 12, tzinfo=ZoneInfo('America/New_York')),
            session=ErrorSession(),
        )

    assert exc.value.status_code == 401
    assert 'Invalid API key' in str(exc.value)

