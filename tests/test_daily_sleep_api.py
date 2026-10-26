import json

import pytest
from django.test import RequestFactory

import fitnick.base.live_api as live_api
from fitnick.base.live_api import HealthAPIError
from fitnick_django.fitnick_django.middleware import AccessControlMiddleware
from fitnick_django.fitnick_django import views


pytestmark = pytest.mark.django


def _request(query=None, method='get'):
    if query is None:
        query = {'from': '2026-10-01', 'to': '2026-10-02'}
    return getattr(RequestFactory(), method)('/api/sleep/daily', data=query)


def _payload(response):
    return json.loads(response.content)


def test_daily_sleep_requires_api_key_when_enabled(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_API_KEY', 'secret')
    monkeypatch.delenv('FITNICK_BASIC_AUTH_USER', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_PASS', raising=False)

    response = AccessControlMiddleware(views.daily_sleep)(_request())
    assert response.status_code == 401


def test_daily_sleep_rejects_invalid_windows(monkeypatch):
    monkeypatch.setattr(
        views,
        'get_daily_sleep_metrics',
        lambda start_date, end_date: pytest.fail('Invalid windows must not reach provider mapper'),
    )
    response = views.daily_sleep(_request({'from': '2026-10-03', 'to': '2026-10-02'}))
    assert response.status_code == 400


def test_daily_sleep_only_allows_get():
    assert views.daily_sleep(_request(method='post')).status_code == 405


def test_daily_sleep_view_maps_rows(monkeypatch):
    monkeypatch.setattr(
        views,
        'get_daily_sleep_metrics',
        lambda start_date, end_date: [
            {
                'on_date': '2026-10-01',
                'sleep_minutes': 440,
                'minutes_asleep': 400,
                'minutes_awake': 40,
            }
        ],
    )
    response = views.daily_sleep(_request({'from': '2026-10-01', 'to': '2026-10-01'}))
    assert response.status_code == 200
    assert _payload(response) == {
        'days': [
            {
                'on_date': '2026-10-01',
                'sleep_minutes': 440,
                'minutes_asleep': 400,
                'minutes_awake': 40,
            }
        ]
    }


def test_daily_sleep_view_translates_upstream_failure(monkeypatch):
    def boom(start_date, end_date):
        raise HealthAPIError(status_code=502, provider='google', error_type='upstream', message='temporary error')

    monkeypatch.setattr(views, 'get_daily_sleep_metrics', boom)
    response = views.daily_sleep(_request({'from': '2026-10-01', 'to': '2026-10-01'}))
    assert response.status_code == 502
    assert _payload(response)['ok'] is False


def test_live_api_google_daily_sleep_mapping(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')

    def fake_provider_get(provider, api_version, path, params=None):
        assert path == 'users/me/dataTypes/sleep/dataPoints:reconcile'
        return {
            'dataPoints': [
                {
                    'sleep': {
                        'interval': {'civilEndTime': {'date': {'year': 2026, 'month': 10, 'day': 2}}},
                        'summary': {'minutesAsleep': '390', 'minutesAwake': '30'},
                    }
                },
                {
                    'sleep': {
                        'interval': {'civilEndTime': {'date': {'year': 2026, 'month': 10, 'day': 2}}},
                        'summary': {'minutesAsleep': '40', 'minutesAwake': '10'},
                    }
                },
            ]
        }

    monkeypatch.setattr(live_api, '_provider_get', fake_provider_get)
    rows = live_api.get_daily_sleep_metrics(
        start_date=live_api.datetime(2026, 10, 1).date(),
        end_date=live_api.datetime(2026, 10, 2).date(),
    )
    assert rows == [
        {
            'on_date': '2026-10-02',
            'sleep_minutes': 470,
            'minutes_asleep': 430,
            'minutes_awake': 40,
        }
    ]


def test_live_api_fitbit_daily_sleep_mapping(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')

    def fake_provider_get(provider, api_version, path, params=None):
        assert path == 'user/-/sleep/date/2026-10-01/2026-10-02.json'
        return {
            'sleep': [
                {'dateOfSleep': '2026-10-01', 'minutesAsleep': 300, 'minutesAwake': 30, 'timeInBed': 330},
                {'dateOfSleep': '2026-10-01', 'minutesAsleep': 50, 'minutesAwake': 20, 'timeInBed': 70},
                {'dateOfSleep': '2026-10-02', 'minutesAsleep': 400, 'minutesAwake': 40},
            ]
        }

    monkeypatch.setattr(live_api, '_provider_get', fake_provider_get)
    rows = live_api.get_daily_sleep_metrics(
        start_date=live_api.datetime(2026, 10, 1).date(),
        end_date=live_api.datetime(2026, 10, 2).date(),
    )
    assert rows == [
        {
            'on_date': '2026-10-01',
            'sleep_minutes': 400,
            'minutes_asleep': 350,
            'minutes_awake': 50,
        },
        {
            'on_date': '2026-10-02',
            'sleep_minutes': 440,
            'minutes_asleep': 400,
            'minutes_awake': 40,
        },
    ]


def test_openapi_contract_includes_daily_sleep():
    spec = _payload(views.openapi_spec(_request()))
    operation = spec['paths']['/api/sleep/daily']['get']
    assert operation['security'] == [{'ApiKeyAuth': []}]
    assert [p['name'] for p in operation['parameters']] == ['from', 'to']
    item = operation['responses']['200']['content']['application/json']['schema']['properties']['days']['items']
    assert set(item['required']) == {'on_date', 'sleep_minutes', 'minutes_asleep', 'minutes_awake'}

