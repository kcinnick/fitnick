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
        lambda start_date, end_date, warnings: pytest.fail('Invalid windows must not reach provider mapper'),
    )
    response = views.daily_sleep(_request({'from': '2026-10-03', 'to': '2026-10-02'}))
    assert response.status_code == 400


def test_daily_sleep_only_allows_get():
    assert views.daily_sleep(_request(method='post')).status_code == 405


def test_daily_sleep_view_maps_rows(monkeypatch):
    monkeypatch.setattr(
        views,
        'get_daily_sleep_metrics',
        lambda start_date, end_date, warnings: [
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


def test_daily_sleep_response_includes_provider_warnings(monkeypatch):
    def mapper(start_date, end_date, warnings):
        warnings.append({
            'code': 'unassigned_sleep_date',
            'message': 'A provider sleep row was omitted.',
        })
        return []

    monkeypatch.setattr(views, 'get_daily_sleep_metrics', mapper)
    response = views.daily_sleep(_request({'from': '2026-10-01', 'to': '2026-10-01'}))
    assert _payload(response) == {
        'days': [],
        'warnings': [{
            'code': 'unassigned_sleep_date',
            'message': 'A provider sleep row was omitted.',
        }],
    }


def test_daily_sleep_view_translates_upstream_failure(monkeypatch):
    def boom(start_date, end_date, warnings):
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


@pytest.mark.parametrize(('end_time', 'expected_date'), [
    ('2026-03-08T04:30:00Z', '2026-03-07'),
    ('2026-03-08T07:30:00Z', '2026-03-08'),
    ('2026-11-01T03:30:00Z', '2026-10-31'),
    ('2026-11-01T06:30:00Z', '2026-11-01'),
])
def test_google_sleep_timestamp_fallback_uses_configured_civil_timezone(
    monkeypatch, end_time, expected_date,
):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    monkeypatch.setenv('FITNICK_TIME_ZONE', 'America/New_York')
    monkeypatch.setattr(live_api, '_provider_get', lambda **kwargs: {
        'dataPoints': [{
            'sleep': {
                'interval': {'endTime': end_time},
                'summary': {'minutesAsleep': 60, 'minutesAwake': 10},
            },
        }],
    })
    warnings = []
    target_date = live_api._coerce_iso_date(expected_date)
    rows = live_api.get_daily_sleep_metrics(
        target_date,
        target_date,
        warnings=warnings,
    )
    assert rows == [{
        'on_date': expected_date,
        'sleep_minutes': 70,
        'minutes_asleep': 60,
        'minutes_awake': 10,
    }]
    assert warnings[0]['code'] == 'sleep_date_timezone_fallback'


def test_google_sleep_naive_timestamp_is_omitted_with_warning(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    monkeypatch.setattr(live_api, '_provider_get', lambda **kwargs: {
        'dataPoints': [{
            'sleep': {
                'interval': {'endTime': '2026-10-01T23:30:00'},
                'summary': {'minutesAsleep': 60, 'minutesAwake': 10},
            },
        }],
    })
    warnings = []
    rows = live_api.get_daily_sleep_metrics(
        live_api.datetime(2026, 10, 1).date(),
        live_api.datetime(2026, 10, 1).date(),
        warnings=warnings,
    )
    assert rows == []
    assert warnings[0]['code'] == 'unassigned_sleep_date'


def test_sleep_partial_summary_does_not_infer_time_in_bed(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')
    monkeypatch.setattr(live_api, '_provider_get', lambda **kwargs: {
        'sleep': [{
            'dateOfSleep': '2026-10-01',
            'minutesAsleep': 300,
        }],
    })
    rows = live_api.get_daily_sleep_metrics(
        live_api.datetime(2026, 10, 1).date(),
        live_api.datetime(2026, 10, 1).date(),
    )
    assert rows == [{
        'on_date': '2026-10-01',
        'sleep_minutes': None,
        'minutes_asleep': 300,
        'minutes_awake': None,
    }]


def test_fitbit_negative_sleep_metrics_are_omitted_with_warning(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')
    monkeypatch.setattr(live_api, '_provider_get', lambda **kwargs: {
        'sleep': [{
            'dateOfSleep': '2026-10-01',
            'minutesAsleep': -1,
            'minutesAwake': 20,
            'timeInBed': 30,
        }],
    })
    warnings = []
    rows = live_api.get_daily_sleep_metrics(
        live_api.datetime(2026, 10, 1).date(),
        live_api.datetime(2026, 10, 1).date(),
        warnings=warnings,
    )
    assert rows == [{
        'on_date': '2026-10-01',
        'sleep_minutes': 30,
        'minutes_asleep': None,
        'minutes_awake': 20,
    }]
    assert warnings[0]['code'] == 'invalid_sleep_metric'


def test_fitbit_multiple_sessions_keep_partially_reported_metrics_null(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')
    monkeypatch.setattr(live_api, '_provider_get', lambda **kwargs: {
        'sleep': [
            {
                'dateOfSleep': '2026-10-01',
                'minutesAsleep': 300,
                'minutesAwake': 20,
                'timeInBed': 320,
            },
            {
                'dateOfSleep': '2026-10-01',
                'minutesAsleep': 30,
                'timeInBed': 40,
            },
        ],
    })
    warnings = []
    rows = live_api.get_daily_sleep_metrics(
        live_api.datetime(2026, 10, 1).date(),
        live_api.datetime(2026, 10, 1).date(),
        warnings=warnings,
    )
    assert rows == [{
        'on_date': '2026-10-01',
        'sleep_minutes': 360,
        'minutes_asleep': 330,
        'minutes_awake': None,
    }]
    assert any(warning['code'] == 'partial_sleep_summary' for warning in warnings)


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
    assert 'warnings' in operation['responses']['200']['content']['application/json']['schema']['properties']
