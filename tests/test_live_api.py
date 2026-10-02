import time

import fitnick.base.live_api as live_api


class DummyResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.ok = 200 <= status_code < 300
        self.text = str(self._payload)

    def json(self):
        return self._payload


def test_get_access_token_refreshes_google_when_missing(monkeypatch):
    monkeypatch.setenv('FITNICK_AUTO_REFRESH_TOKENS', '1')
    monkeypatch.setenv('GOOGLE_HEALTH_CLIENT_ID', 'cid')
    monkeypatch.setenv('GOOGLE_HEALTH_CLIENT_SECRET', 'csecret')
    monkeypatch.setenv('GOOGLE_HEALTH_REFRESH_TOKEN', 'rtok')
    monkeypatch.delenv('GOOGLE_HEALTH_ACCESS_TOKEN', raising=False)
    monkeypatch.delenv('HEALTH_ACCESS_TOKEN', raising=False)

    def fake_post(url, data=None, timeout=0, **kwargs):
        assert url == live_api.GOOGLE_TOKEN_URL
        assert data['grant_type'] == 'refresh_token'
        return DummyResponse(payload={'access_token': 'new-access-token'})

    monkeypatch.setattr('fitnick.base.live_api.requests.post', fake_post)

    token = live_api._get_access_token('google')
    assert token == 'new-access-token'
    assert live_api._get_google_access_token() == 'new-access-token'


def test_provider_get_retries_once_after_401(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    monkeypatch.setenv('FITNICK_AUTO_REFRESH_TOKENS', '1')
    monkeypatch.setenv('FITNICK_PROACTIVE_REFRESH_TOKENS', '0')
    monkeypatch.setenv('GOOGLE_HEALTH_ACCESS_TOKEN', 'stale-token')
    monkeypatch.setenv('GOOGLE_HEALTH_CLIENT_ID', 'cid')
    monkeypatch.setenv('GOOGLE_HEALTH_CLIENT_SECRET', 'csecret')
    monkeypatch.setenv('GOOGLE_HEALTH_REFRESH_TOKEN', 'rtok')

    request_tokens = []

    def fake_get(url, headers=None, params=None, timeout=0, **kwargs):
        request_tokens.append(headers.get('Authorization', ''))
        if len(request_tokens) == 1:
            return DummyResponse(status_code=401, payload={'error': {'status': 'UNAUTHENTICATED', 'message': 'expired'}})
        return DummyResponse(payload={'healthUserId': 'abc123', 'legacyUserId': 'legacy-1'})

    def fake_post(url, data=None, timeout=0, **kwargs):
        if url == live_api.GOOGLE_TOKEN_URL:
            return DummyResponse(payload={'access_token': 'fresh-token'})
        return DummyResponse(payload={})

    monkeypatch.setattr('fitnick.base.live_api.requests.get', fake_get)
    monkeypatch.setattr('fitnick.base.live_api.requests.post', fake_post)

    identity = live_api.get_identity_summary()
    assert identity['provider'] == 'google'
    assert identity['health_user_id'] == 'abc123'
    assert request_tokens == ['Bearer stale-token', 'Bearer fresh-token']


def test_refresh_sets_google_token_expiry(monkeypatch):
    monkeypatch.setenv('FITNICK_AUTO_REFRESH_TOKENS', '1')
    monkeypatch.setenv('GOOGLE_HEALTH_CLIENT_ID', 'cid')
    monkeypatch.setenv('GOOGLE_HEALTH_CLIENT_SECRET', 'csecret')
    monkeypatch.setenv('GOOGLE_HEALTH_REFRESH_TOKEN', 'rtok')
    monkeypatch.delenv('GOOGLE_HEALTH_ACCESS_TOKEN', raising=False)
    monkeypatch.delenv('HEALTH_ACCESS_TOKEN', raising=False)
    monkeypatch.delenv('GOOGLE_HEALTH_ACCESS_TOKEN_EXPIRES_AT', raising=False)

    def fake_post(url, data=None, timeout=0, **kwargs):
        return DummyResponse(payload={'access_token': 'new-access-token', 'expires_in': 3600})

    monkeypatch.setattr('fitnick.base.live_api.requests.post', fake_post)

    token = live_api._get_access_token('google')
    expires_at = int(live_api.os.getenv('GOOGLE_HEALTH_ACCESS_TOKEN_EXPIRES_AT', '0'))

    assert token == 'new-access-token'
    assert expires_at > int(time.time())


def test_provider_get_proactively_refreshes_when_expiring(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    monkeypatch.setenv('FITNICK_AUTO_REFRESH_TOKENS', '1')
    monkeypatch.setenv('FITNICK_PROACTIVE_REFRESH_TOKENS', '1')
    monkeypatch.setenv('FITNICK_TOKEN_REFRESH_BUFFER_SECONDS', '300')
    monkeypatch.setenv('GOOGLE_HEALTH_ACCESS_TOKEN', 'stale-token')
    monkeypatch.setenv('GOOGLE_HEALTH_ACCESS_TOKEN_EXPIRES_AT', str(int(time.time()) - 10))
    monkeypatch.setenv('GOOGLE_HEALTH_CLIENT_ID', 'cid')
    monkeypatch.setenv('GOOGLE_HEALTH_CLIENT_SECRET', 'csecret')
    monkeypatch.setenv('GOOGLE_HEALTH_REFRESH_TOKEN', 'rtok')

    request_tokens = []

    def fake_get(url, headers=None, params=None, timeout=0, **kwargs):
        request_tokens.append(headers.get('Authorization', ''))
        return DummyResponse(payload={'healthUserId': 'abc123', 'legacyUserId': 'legacy-1'})

    def fake_post(url, data=None, timeout=0, **kwargs):
        return DummyResponse(payload={'access_token': 'fresh-token', 'expires_in': 3600})

    monkeypatch.setattr('fitnick.base.live_api.requests.get', fake_get)
    monkeypatch.setattr('fitnick.base.live_api.requests.post', fake_post)

    identity = live_api.get_identity_summary()

    assert identity['provider'] == 'google'
    assert request_tokens == ['Bearer fresh-token']

def test_uses_live_health_api_returns_false_when_token_missing(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    monkeypatch.setenv('FITNICK_OFFLINE_MODE', '0')
    monkeypatch.setenv('FITNICK_AUTO_REFRESH_TOKENS', '0')
    monkeypatch.delenv('GOOGLE_HEALTH_ACCESS_TOKEN', raising=False)
    monkeypatch.delenv('HEALTH_ACCESS_TOKEN', raising=False)
    monkeypatch.delenv('GOOGLE_HEALTH_REFRESH_TOKEN', raising=False)
    monkeypatch.delenv('GOOGLE_HEALTH_CLIENT_ID', raising=False)
    monkeypatch.delenv('GOOGLE_HEALTH_CLIENT_SECRET', raising=False)

    assert live_api.uses_live_health_api() is False


def test_google_rollup_uses_all_health_data_sources(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    monkeypatch.delenv('GOOGLE_HEALTH_DATA_SOURCE_FAMILY', raising=False)
    captured = {}

    def fake_provider_post(provider, api_version, path, payload):
        captured['family'] = payload['dataSourceFamily']
        return {'rollupDataPoints': []}

    monkeypatch.setattr('fitnick.base.live_api._provider_post', fake_provider_post)
    live_api._google_daily_steps('2026-09-11')

    assert captured['family'] == 'users/me/dataSourceFamilies/all-sources'


def test_google_body_weight_maps_kilograms_to_provider_rows(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')

    def fake_provider_get(provider, api_version, path, params=None):
        assert provider == 'google'
        assert path == 'users/me/dataTypes/weight/dataPoints'
        return {
            'dataPoints': [{
                'weight': {
                    'sampleTime': {'physicalTime': '2026-09-11T08:00:00Z'},
                    'kilograms': 80,
                }
            }]
        }

    monkeypatch.setattr('fitnick.base.live_api._provider_get', fake_provider_get)

    assert live_api.get_body_weight(
        live_api.datetime(2026, 9, 11).date(),
        live_api.datetime(2026, 9, 11).date(),
    ) == [{'date': '2026-09-11', 'kilograms': 80.0}]


def test_latest_sleep_session_preserves_wake_time(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    monkeypatch.setattr(
        'fitnick.base.live_api._provider_get',
        lambda **kwargs: {
            'dataPoints': [
                {
                    'sleep': {
                        'interval': {'endTime': '2026-08-29T07:15:00-04:00'},
                        'summary': {'minutesAsleep': 420, 'minutesAwake': 20},
                    }
                }
            ]
        },
    )

    result = live_api.get_latest_sleep_session()

    assert result == {
        'date': '2026-08-29',
        'wake_time': '2026-08-29T07:15:00-04:00',
        'minutes_asleep': 420,
        'minutes_awake': 20,
    }


def _sleep_row(start_time, end_time, minutes_asleep, minutes_awake=0):
    return {
        'sleep': {
            'interval': {'startTime': start_time, 'endTime': end_time},
            'summary': {'minutesAsleep': minutes_asleep, 'minutesAwake': minutes_awake},
        }
    }


def test_sleep_selection_keeps_longest_overnight_instead_of_later_nap(monkeypatch):
    monkeypatch.setenv('FITNICK_OVERNIGHT_SLEEP_MINUTES', '180')
    overnight = _sleep_row(
        '2026-08-29T03:30:00-04:00',
        '2026-08-29T13:57:00-04:00',
        590,
        37,
    )
    later_nap = _sleep_row(
        '2026-08-29T16:00:00-04:00',
        '2026-08-29T17:00:00-04:00',
        60,
    )

    assert live_api._select_latest_overnight_sleep([overnight, later_nap]) is overnight


def test_sleep_selection_prefers_latest_overnight_date_over_longer_old_session(monkeypatch):
    monkeypatch.setenv('FITNICK_OVERNIGHT_SLEEP_MINUTES', '180')
    older_longer = _sleep_row(
        '2026-08-27T22:00:00-04:00',
        '2026-08-28T08:00:00-04:00',
        580,
    )
    latest = _sleep_row(
        '2026-08-28T23:30:00-04:00',
        '2026-08-29T07:00:00-04:00',
        430,
    )

    assert live_api._select_latest_overnight_sleep([older_longer, latest]) is latest


def test_sleep_selection_falls_back_to_longest_latest_short_session(monkeypatch):
    monkeypatch.setenv('FITNICK_OVERNIGHT_SLEEP_MINUTES', '180')
    shorter = _sleep_row('2026-08-29T11:00:00-04:00', '2026-08-29T11:20:00-04:00', 20)
    longer = _sleep_row('2026-08-29T12:00:00-04:00', '2026-08-29T13:00:00-04:00', 60)

    assert live_api._select_latest_overnight_sleep([shorter, longer]) is longer
