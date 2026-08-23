import base64

from django.conf import settings

from fitnick_django.fitnick_django.middleware import AccessControlMiddleware


if not settings.configured:
    settings.configure(DEFAULT_CHARSET='utf-8')


class DummyRequest:
    def __init__(self, path='/', headers=None, user=None):
        self.path = path
        self.headers = headers or {}
        self.user = user


class DummyUser:
    def __init__(self, is_authenticated=False):
        self.is_authenticated = is_authenticated


def test_auth_disabled_allows_request(monkeypatch):
    monkeypatch.delenv('FITNICK_REQUIRE_AUTH', raising=False)
    middleware = AccessControlMiddleware(lambda request: 'ok')
    assert middleware(DummyRequest()) == 'ok'


def test_api_key_auth_enforced(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_API_KEY', 'secret-key')
    monkeypatch.delenv('FITNICK_BASIC_AUTH_USER', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_PASS', raising=False)

    middleware = AccessControlMiddleware(lambda request: 'ok')

    unauthorized = middleware(DummyRequest(path='/', headers={}))
    assert unauthorized.status_code == 401

    authorized = middleware(DummyRequest(path='/', headers={'X-API-Key': 'secret-key'}))
    assert authorized == 'ok'


def test_basic_auth_and_exempt_path(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_AUTH_EXEMPT_PATHS', '/healthz')
    monkeypatch.delenv('FITNICK_API_KEY', raising=False)
    monkeypatch.setenv('FITNICK_BASIC_AUTH_USER', 'nick')
    monkeypatch.setenv('FITNICK_BASIC_AUTH_PASS', 'pw123')

    middleware = AccessControlMiddleware(lambda request: 'ok')

    # Health checks remain public so Render can monitor service liveness.
    assert middleware(DummyRequest(path='/healthz')) == 'ok'

    header_value = 'Basic ' + base64.b64encode(b'nick:pw123').decode('ascii')
    assert middleware(DummyRequest(path='/', headers={'Authorization': header_value})) == 'ok'


def test_session_authenticated_user_bypasses_header_auth(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.delenv('FITNICK_API_KEY', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_USER', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_PASS', raising=False)

    middleware = AccessControlMiddleware(lambda request: 'ok')
    assert middleware(DummyRequest(path='/', user=DummyUser(is_authenticated=True))) == 'ok'


def test_default_exempt_login_path(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.delenv('FITNICK_AUTH_EXEMPT_PATHS', raising=False)
    monkeypatch.delenv('FITNICK_API_KEY', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_USER', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_PASS', raising=False)

    middleware = AccessControlMiddleware(lambda request: 'ok')
    assert middleware(DummyRequest(path='/login')) == 'ok'


