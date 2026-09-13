import json

import fitnick.base.live_api as live_api
from fitnick.base.live_api import HealthAPIError
from fitnick_django.fitnick_django.middleware import AccessControlMiddleware
from fitnick_django.fitnick_django.views import daily_heart_rate


class DummyUser:
    def __init__(self, is_authenticated=False):
        self.is_authenticated = is_authenticated


class DummyRequest:
    def __init__(self, path, query=None, headers=None, user=None):
        self.path = path
        self.GET = query or {}
        self.headers = headers or {}
        self.user = user or DummyUser(is_authenticated=False)


def _decode_json(response):
    return json.loads(response.content.decode('utf-8'))


def test_daily_endpoint_requires_auth_when_enabled(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_API_KEY', 'secret-key')
    monkeypatch.delenv('FITNICK_BASIC_AUTH_USER', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_PASS', raising=False)

    middleware = AccessControlMiddleware(lambda request: daily_heart_rate(request))

    response = middleware(
        DummyRequest(
            path='/api/heart-rate/daily',
            query={'from': '2026-09-11', 'to': '2026-09-11'},
            headers={},
        )
    )
    assert response.status_code == 401


def test_daily_endpoint_rejects_invalid_api_key(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_API_KEY', 'secret-key')
    monkeypatch.delenv('FITNICK_BASIC_AUTH_USER', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_PASS', raising=False)

    middleware = AccessControlMiddleware(lambda request: daily_heart_rate(request))

    response = middleware(
        DummyRequest(
            path='/api/heart-rate/daily',
            query={'from': '2026-09-11', 'to': '2026-09-11'},
            headers={'X-API-Key': 'wrong-key'},
        )
    )
    assert response.status_code == 401


def test_daily_endpoint_allows_valid_api_key(monkeypatch):
    monkeypatch.setenv('FITNICK_REQUIRE_AUTH', '1')
    monkeypatch.setenv('FITNICK_API_KEY', 'secret-key')
    monkeypatch.delenv('FITNICK_BASIC_AUTH_USER', raising=False)
    monkeypatch.delenv('FITNICK_BASIC_AUTH_PASS', raising=False)
    monkeypatch.setattr(
        'fitnick_django.fitnick_django.views.get_daily_heart_rate_metrics',
        lambda start_date, end_date: [
            {
                'on_date': '2026-09-11',
                'resting_bpm': 62,
                'avg_bpm': None,
                'min_bpm': None,
                'max_bpm': None,
                'hrv_ms': None,
            }
        ],
    )

    middleware = AccessControlMiddleware(lambda request: daily_heart_rate(request))

    response = middleware(
        DummyRequest(
            path='/api/heart-rate/daily',
            query={'from': '2026-09-11', 'to': '2026-09-11'},
            headers={'X-API-Key': 'secret-key'},
        )
    )
    assert response.status_code == 200


def test_daily_range_validation_one_day(monkeypatch):
    monkeypatch.setattr(
        'fitnick_django.fitnick_django.views.get_daily_heart_rate_metrics',
        lambda start_date, end_date: [
            {
                'on_date': '2026-09-11',
                'resting_bpm': 62,
                'avg_bpm': 78,
                'min_bpm': 51,
                'max_bpm': 138,
                'hrv_ms': 42.5,
            }
        ],
    )
    response = daily_heart_rate(DummyRequest(path='/api/heart-rate/daily', query={'from': '2026-09-11', 'to': '2026-09-11'}))
    payload = _decode_json(response)

    assert response.status_code == 200
    assert payload['days'][0]['on_date'] == '2026-09-11'


def test_daily_range_validation_multi_day_inclusive(monkeypatch):
    captured = {}

    def fake_get_daily_heart_rate_metrics(start_date, end_date):
        captured['start'] = start_date.strftime('%Y-%m-%d')
        captured['end'] = end_date.strftime('%Y-%m-%d')
        return []

    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_daily_heart_rate_metrics', fake_get_daily_heart_rate_metrics)
    response = daily_heart_rate(DummyRequest(path='/api/heart-rate/daily', query={'from': '2026-09-10', 'to': '2026-09-11'}))

    assert response.status_code == 200
    assert captured == {'start': '2026-09-10', 'end': '2026-09-11'}


def test_daily_range_validation_invalid_date():
    response = daily_heart_rate(DummyRequest(path='/api/heart-rate/daily', query={'from': '2026-09-xx', 'to': '2026-09-11'}))
    assert response.status_code == 400


def test_daily_range_validation_reversed_range():
    response = daily_heart_rate(DummyRequest(path='/api/heart-rate/daily', query={'from': '2026-09-12', 'to': '2026-09-11'}))
    assert response.status_code == 400


def test_daily_range_validation_over_90_days():
    response = daily_heart_rate(DummyRequest(path='/api/heart-rate/daily', query={'from': '2026-01-01', 'to': '2026-04-01'}))
    assert response.status_code == 400


def test_live_api_maps_full_daily_metrics(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')

    def fake_provider_get(provider, api_version, path, params=None):
        if path == 'user/-/activities/heart/date/2026-09-11/2026-09-11.json':
            return {'activities-heart': [{'dateTime': '2026-09-11', 'value': {'restingHeartRate': 62}}]}
        if path == 'user/-/activities/heart/date/2026-09-11/1d/1min.json':
            return {'activities-heart-intraday': {'dataset': [{'value': 60}, {'value': 80}, {'value': 100}]}}
        if path == 'user/-/hrv/date/2026-09-11.json':
            return {'hrv': [{'dateTime': '2026-09-11', 'value': {'dailyRmssd': 42.5}}]}
        raise AssertionError(f'Unexpected path: {path}')

    monkeypatch.setattr('fitnick.base.live_api._provider_get', fake_provider_get)

    rows = live_api.get_daily_heart_rate_metrics(start_date=live_api.datetime(2026, 9, 11).date(), end_date=live_api.datetime(2026, 9, 11).date())
    assert rows == [
        {
            'on_date': '2026-09-11',
            'resting_bpm': 62,
            'avg_bpm': 80,
            'min_bpm': 60,
            'max_bpm': 100,
            'hrv_ms': 42.5,
        }
    ]


def test_live_api_keeps_missing_values_null(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')

    def fake_provider_get(provider, api_version, path, params=None):
        if path == 'user/-/activities/heart/date/2026-09-11/2026-09-11.json':
            return {'activities-heart': [{'dateTime': '2026-09-11', 'value': {}}]}
        if path == 'user/-/activities/heart/date/2026-09-11/1d/1min.json':
            return {'activities-heart-intraday': {'dataset': []}}
        if path == 'user/-/hrv/date/2026-09-11.json':
            return {'hrv': [{'dateTime': '2026-09-11', 'value': {}}]}
        raise AssertionError(f'Unexpected path: {path}')

    monkeypatch.setattr('fitnick.base.live_api._provider_get', fake_provider_get)

    rows = live_api.get_daily_heart_rate_metrics(start_date=live_api.datetime(2026, 9, 11).date(), end_date=live_api.datetime(2026, 9, 11).date())
    assert rows == []


def test_live_api_empty_fitbit_results(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')

    def fake_provider_get(provider, api_version, path, params=None):
        if path == 'user/-/activities/heart/date/2026-09-11/2026-09-11.json':
            return {'activities-heart': []}
        raise HealthAPIError(status_code=404, provider='fitbit', error_type='not_found', message='not found')

    monkeypatch.setattr('fitnick.base.live_api._provider_get', fake_provider_get)

    rows = live_api.get_daily_heart_rate_metrics(start_date=live_api.datetime(2026, 9, 11).date(), end_date=live_api.datetime(2026, 9, 11).date())
    assert rows == []


def test_live_api_results_are_sorted_by_date(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'fitbit')

    def fake_provider_get(provider, api_version, path, params=None):
        if path == 'user/-/activities/heart/date/2026-09-10/2026-09-11.json':
            return {
                'activities-heart': [
                    {'dateTime': '2026-09-11', 'value': {'restingHeartRate': 62}},
                    {'dateTime': '2026-09-10', 'value': {'restingHeartRate': 61}},
                ]
            }
        if path.endswith('/1d/1min.json'):
            return {'activities-heart-intraday': {'dataset': []}}
        if path.startswith('user/-/hrv/date/'):
            return {'hrv': []}
        raise AssertionError(f'Unexpected path: {path}')

    monkeypatch.setattr('fitnick.base.live_api._provider_get', fake_provider_get)

    rows = live_api.get_daily_heart_rate_metrics(start_date=live_api.datetime(2026, 9, 10).date(), end_date=live_api.datetime(2026, 9, 11).date())
    assert [row['on_date'] for row in rows] == ['2026-09-10', '2026-09-11']


def test_daily_view_translates_upstream_failure(monkeypatch):
    def fail_metrics(start_date, end_date):
        raise HealthAPIError(status_code=502, provider='fitbit', error_type='upstream', message='temporary error')

    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_daily_heart_rate_metrics', fail_metrics)

    response = daily_heart_rate(DummyRequest(path='/api/heart-rate/daily', query={'from': '2026-09-11', 'to': '2026-09-11'}))
    payload = _decode_json(response)

    assert response.status_code == 502
    assert payload['ok'] is False
    assert 'temporary error' in payload['error']


def test_live_api_maps_google_daily_resting_and_hrv(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')

    def fake_provider_get(provider, api_version, path, params=None):
        assert provider == 'google'
        if path == 'users/me/dataTypes/daily-resting-heart-rate/dataPoints':
            return {
                'dataPoints': [
                    {
                        'dailyRestingHeartRate': {
                            'date': {'year': 2026, 'month': 9, 'day': 11},
                            'beatsPerMinute': '62',
                        }
                    }
                ]
            }
        if path == 'users/me/dataTypes/daily-heart-rate-variability/dataPoints':
            return {
                'dataPoints': [
                    {
                        'dailyHeartRateVariability': {
                            'date': {'year': 2026, 'month': 9, 'day': 11},
                            'averageHeartRateVariabilityMilliseconds': 41.25,
                        }
                    }
                ]
            }
        raise AssertionError(f'Unexpected path: {path}')

    monkeypatch.setattr('fitnick.base.live_api._provider_get', fake_provider_get)

    rows = live_api.get_daily_heart_rate_metrics(
        start_date=live_api.datetime(2026, 9, 11).date(),
        end_date=live_api.datetime(2026, 9, 11).date(),
    )
    assert rows == [
        {
            'on_date': '2026-09-11',
            'resting_bpm': 62,
            'avg_bpm': None,
            'min_bpm': None,
            'max_bpm': None,
            'hrv_ms': 41.25,
        }
    ]


def test_live_api_google_daily_filter_fallback(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    calls = {'resting': 0, 'hrv': 0}

    def fake_provider_get(provider, api_version, path, params=None):
        assert provider == 'google'
        if path == 'users/me/dataTypes/daily-resting-heart-rate/dataPoints':
            calls['resting'] += 1
            if params:
                raise HealthAPIError(status_code=400, provider='google', error_type='invalid_argument', message='bad filter')
            return {'dataPoints': []}
        if path == 'users/me/dataTypes/daily-heart-rate-variability/dataPoints':
            calls['hrv'] += 1
            if params:
                raise HealthAPIError(status_code=400, provider='google', error_type='invalid_argument', message='bad filter')
            return {'dataPoints': []}
        raise AssertionError(f'Unexpected path: {path}')

    monkeypatch.setattr('fitnick.base.live_api._provider_get', fake_provider_get)
    rows = live_api.get_daily_heart_rate_metrics(
        start_date=live_api.datetime(2026, 9, 11).date(),
        end_date=live_api.datetime(2026, 9, 11).date(),
    )

    assert rows == []
    assert calls == {'resting': 2, 'hrv': 2}


