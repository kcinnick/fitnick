import json
from datetime import date, timedelta

import pytest
from django.test import RequestFactory

from fitnick.base import live_api
from fitnick_django.fitnick_django import views
from fitnick_django.fitnick_django.middleware import AccessControlMiddleware


pytestmark = pytest.mark.django
ENDPOINTS = [
    ('/api/steps/daily', views.daily_steps),
    ('/api/goals/progress', views.daily_goal_progress),
]


def _request(path, query=None, method='get'):
    if query is None:
        query = {'from': '2026-10-01', 'to': '2026-10-02'}
    return getattr(RequestFactory(), method)(
        path, data=query,
    )


def _payload(response):
    return json.loads(response.content)


@pytest.fixture(autouse=True)
def isolate_goal_settings(monkeypatch, tmp_path):
    monkeypatch.setenv(
        'FITNICK_SETTINGS_FILE', str(tmp_path / 'settings.json'),
    )
    monkeypatch.setenv('FITNICK_DEFAULT_STEPS_GOAL', '10000')


@pytest.mark.parametrize('path,view', ENDPOINTS)
def test_daily_rows_include_both_bounds(monkeypatch, path, view):
    calls = []

    def activity(on_date):
        calls.append(on_date)
        return {'summary': {'steps': 4500}, 'goals': {'steps': 9000}}

    monkeypatch.setattr(views, 'get_daily_activity_summary', activity)
    response = view(_request(path))
    assert response.status_code == 200
    assert calls == ['2026-10-01', '2026-10-02']
    rows = _payload(response)['days']
    assert [row['on_date'] for row in rows] == calls
    assert all(row['steps'] == 4500 for row in rows)
    if view == views.daily_goal_progress:
        assert rows[0] == {
            'on_date': '2026-10-01', 'steps': 4500, 'steps_goal': 9000,
            'remaining_steps': 4500, 'percent': 50.0, 'goal_reached': False,
        }
    else:
        assert set(rows[0]) == {'on_date', 'steps'}


@pytest.mark.parametrize('steps,goal,reached,remaining,percent', [
    (10000, 10000, True, 0, 100.0),
    (12000, 10000, True, 0, 120.0),
    (1000, 3000, False, 2000, 33.33),
])
def test_goal_override_and_progress(
    monkeypatch, steps, goal, reached, remaining, percent,
):
    views._set_steps_goal_override(goal)
    monkeypatch.setattr(views, 'get_daily_activity_summary', lambda day: {
        'summary': {'steps': steps}, 'goals': {'steps': 5000},
    })
    response = views.daily_goal_progress(_request('/api/goals/progress'))
    row = _payload(response)['days'][0]
    assert row['steps_goal'] == goal
    assert row['remaining_steps'] == remaining
    assert row['percent'] == percent
    assert row['goal_reached'] is reached


def test_missing_steps_and_goal_follow_dashboard_defaults(monkeypatch):
    monkeypatch.setattr(views, 'get_daily_activity_summary', lambda day: {})
    response = views.daily_goal_progress(_request('/api/goals/progress'))
    row = _payload(response)['days'][0]
    assert row['steps'] == 0
    assert row['steps_goal'] == 10000
    assert row['goal_reached'] is False


@pytest.mark.parametrize('path,view', ENDPOINTS)
@pytest.mark.parametrize('query', [
    {}, {'from': '2026-10-01'}, {'to': '2026-10-01'},
    {'from': '2026-02-30', 'to': '2026-10-01'},
    {'from': 'invalid', 'to': '2026-10-01'},
    {'from': '2026-10-02', 'to': '2026-10-01'},
    {'from': '2026-01-01', 'to': '2026-04-01'},
])
def test_invalid_window_does_not_call_provider(monkeypatch, path, view, query):
    def unexpected(day):
        pytest.fail('Invalid windows must not reach the health provider')

    monkeypatch.setattr(views, 'get_daily_activity_summary', unexpected)
    assert view(_request(path, query)).status_code == 400


@pytest.mark.parametrize('count', [1, 90])
def test_allowed_window_lengths(monkeypatch, count):
    calls = []
    monkeypatch.setattr(
        views, 'get_daily_activity_summary',
        lambda day: calls.append(day) or {},
    )
    start = date(2026, 1, 1)
    response = views.daily_steps(_request('/api/steps/daily', {
        'from': start.isoformat(),
        'to': (start + timedelta(days=count - 1)).isoformat(),
    }))
    assert response.status_code == 200
    assert len(_payload(response)['days']) == count
    assert len(calls) == count


@pytest.mark.parametrize('path,view', ENDPOINTS)
@pytest.mark.parametrize('error,status', [
    (live_api.HealthAPIError(502, 'google', 'upstream', 'Unavailable'), 502),
    (live_api.HealthConfigurationError('Not configured'), 503),
])
def test_provider_failure_is_not_a_partial_success(
    monkeypatch, path, view, error, status,
):
    def activity(day):
        if day == '2026-10-02':
            raise error
        return {'summary': {'steps': 1}}

    monkeypatch.setattr(views, 'get_daily_activity_summary', activity)
    response = view(_request(path))
    assert response.status_code == status
    assert _payload(response)['ok'] is False
    assert 'days' not in _payload(response)


@pytest.mark.parametrize('path,view', ENDPOINTS)
@pytest.mark.parametrize('key,status', [
    (None, 401), ('wrong-key', 401), ('test-key', 200),
])
def test_api_key_auth(monkeypatch, path, view, key, status):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_API_KEY', 'test-key')
    monkeypatch.delenv('FITNICK_AUTH_EXEMPT_PATHS', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_USER', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_PASS', raising=False)
    monkeypatch.setattr(views, 'get_daily_activity_summary', lambda day: {})
    request = _request(path)
    if key:
        request.META['HTTP_X_API_KEY'] = key
    assert AccessControlMiddleware(view)(request).status_code == status


@pytest.mark.parametrize('path,view', ENDPOINTS)
def test_only_get_supported(path, view):
    assert view(_request(path, method='post')).status_code == 405


@pytest.mark.parametrize('provider', ['google', 'fitbit'])
def test_steps_use_live_provider_mapping(monkeypatch, provider):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', provider)
    if provider == 'google':
        def post(**kwargs):
            assert kwargs['path'] == (
                'users/me/dataTypes/steps/dataPoints:dailyRollUp'
            )
            assert kwargs['payload']['range']['end']['date']['day'] == 2
            return {'rollupDataPoints': [{'steps': {'countSum': '1234'}}]}
        monkeypatch.setattr(live_api, '_provider_post', post)
    else:
        def get(**kwargs):
            assert kwargs['path'] == 'user/-/activities/date/2026-10-01.json'
            return {'summary': {'steps': 1234}}
        monkeypatch.setattr(live_api, '_provider_get', get)
    response = views.daily_steps(_request('/api/steps/daily', {
        'from': '2026-10-01', 'to': '2026-10-01',
    }))
    assert _payload(response) == {
        'days': [{'on_date': '2026-10-01', 'steps': 1234}],
    }


@pytest.mark.parametrize('path,view', ENDPOINTS)
def test_openapi_contract(path, view):
    spec = _payload(views.openapi_spec(_request('/openapi.json')))
    operation = spec['paths'][path]['get']
    assert operation['security'] == [{'ApiKeyAuth': []}]
    assert [p['name'] for p in operation['parameters']] == ['from', 'to']
    content = operation['responses']['200']['content']['application/json']
    schema = content['schema']
    assert 'on_date' in schema['properties']['days']['items']['required']
