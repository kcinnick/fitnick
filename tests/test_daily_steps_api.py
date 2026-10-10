import json

import pytest

import fitnick.base.live_api as live_api
from fitnick.base.live_api import HealthAPIError
from fitnick_django.fitnick_django.middleware import AccessControlMiddleware
from fitnick_django.fitnick_django.views import daily_steps

pytestmark = pytest.mark.django


class DummyUser:
    is_authenticated = False


class DummyRequest:
    def __init__(self, query=None, headers=None):
        self.path = '/api/steps/daily'
        self.GET = query or {}
        self.headers = headers or {}
        self.user = DummyUser()


def _json(response):
    return json.loads(response.content.decode('utf-8'))


def _patch_goal(monkeypatch, tmp_path, goal=10000):
    monkeypatch.setenv('FITNICK_SETTINGS_FILE', str(tmp_path / 's.json'))
    monkeypatch.setenv('FITNICK_DEFAULT_STEPS_GOAL', str(goal))


def test_requires_api_key(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_API_KEY', 'secret')
    middleware = AccessControlMiddleware(daily_steps)
    q = {'from': '2026-09-11', 'to': '2026-09-11'}
    assert middleware(DummyRequest(q)).status_code == 401
    assert middleware(DummyRequest(q, {'X-API-Key': 'nope'})).status_code == 401


def test_valid_key_allowed(monkeypatch, tmp_path):
    _patch_goal(monkeypatch, tmp_path)
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_API_KEY', 'secret')
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_daily_steps_metrics',
                        lambda start_date, end_date: [])
    middleware = AccessControlMiddleware(daily_steps)
    response = middleware(DummyRequest({'from': '2026-09-11', 'to': '2026-09-11'}, {'X-API-Key': 'secret'}))
    assert response.status_code == 200
    assert _json(response) == {'goal': 10000, 'days': []}


@pytest.mark.parametrize('query', [
    {'from': '2026-09-xx', 'to': '2026-09-11'},
    {'from': '2026-09-12', 'to': '2026-09-11'},
    {'from': '2026-01-01', 'to': '2026-04-01'},
    {'to': '2026-09-11'},
])
def test_validation_errors(query):
    assert daily_steps(DummyRequest(query)).status_code == 400


def test_maps_sorted_with_goal_met(monkeypatch, tmp_path):
    _patch_goal(monkeypatch, tmp_path)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_daily_steps_metrics', lambda start_date, end_date: [
        {'on_date': '2026-09-11', 'steps': 10000},
        {'on_date': '2026-09-10', 'steps': 4200},
    ])
    payload = _json(daily_steps(DummyRequest({'from': '2026-09-10', 'to': '2026-09-11'})))
    assert payload['days'] == [
        {'on_date': '2026-09-10', 'steps': 4200, 'goal_met': False},
        {'on_date': '2026-09-11', 'steps': 10000, 'goal_met': True},
    ]


def test_upstream_failure_is_safe(monkeypatch):
    def boom(start_date, end_date):
        raise HealthAPIError(status_code=502, provider='google', error_type='upstream', message='temporary error')
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_daily_steps_metrics', boom)
    response = daily_steps(DummyRequest({'from': '2026-09-11', 'to': '2026-09-11'}))
    assert response.status_code == 502
    assert _json(response)['ok'] is False


def test_live_google_rollup_maps_and_omits_missing(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    captured = {}

    def fake_rollup(start_date, end_date):
        captured['end'] = end_date
        return {'rollupDataPoints': [
            {'civilStartTime': {'date': {'year': 2026, 'month': 9, 'day': 11}}, 'steps': {'countSum': '8000'}},
            {'civilStartTime': {'date': {'year': 2026, 'month': 9, 'day': 10}}, 'steps': {'countSum': '3000'}},
            {'civilStartTime': {'date': {'year': 2026, 'month': 9, 'day': 12}}, 'steps': {}},
        ]}

    monkeypatch.setattr('fitnick.base.live_api._google_daily_steps_rollup', fake_rollup)
    d = live_api.datetime
    rows = live_api.get_daily_steps_metrics(d(2026, 9, 10).date(), d(2026, 9, 12).date())
    assert rows == [{'on_date': '2026-09-10', 'steps': 3000}, {'on_date': '2026-09-11', 'steps': 8000}]
    assert captured['end'] == d(2026, 9, 13).date()


def test_live_fitbit_per_day(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')

    def fake_get(provider, api_version, path, params=None):
        if path.endswith('2026-09-10.json'):
            return {'summary': {'steps': 500}}
        return {'summary': {}}

    monkeypatch.setattr('fitnick.base.live_api._provider_get', fake_get)
    d = live_api.datetime
    rows = live_api.get_daily_steps_metrics(d(2026, 9, 10).date(), d(2026, 9, 11).date())
    assert rows == [{'on_date': '2026-09-10', 'steps': 500}]

