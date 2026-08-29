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

