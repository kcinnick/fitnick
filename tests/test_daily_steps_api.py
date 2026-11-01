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
    monkeypatch.setattr(
        views, 'get_daily_steps_metrics',
        lambda start_date, end_date, warnings: [
            {'on_date': day.isoformat(), 'steps': 4500}
            for day in (start_date + timedelta(days=offset)
                        for offset in range((end_date - start_date).days + 1))
        ],
    )
    response = view(_request(path))
    assert response.status_code == 200
    rows = _payload(response)['days']
    assert [row['on_date'] for row in rows] == ['2026-10-01', '2026-10-02']
    assert all(row['steps'] == 4500 for row in rows)
    if view == views.daily_goal_progress:
        assert calls == ['2026-10-01', '2026-10-02']
        assert rows[0] == {
            'on_date': '2026-10-01', 'steps': 4500, 'steps_goal': 9000,
            'remaining_steps': 4500, 'percent': 50.0, 'goal_reached': False,
        }
    else:
        assert calls == []
        assert set(rows[0]) == {'on_date', 'steps'}


def test_daily_steps_returns_null_for_unobserved_dates(monkeypatch):
    monkeypatch.setattr(
        views, 'get_daily_steps_metrics',
        lambda start_date, end_date, warnings: [
            {'on_date': '2026-10-01', 'steps': 0},
        ],
    )
    response = views.daily_steps(_request('/api/steps/daily'))
    assert _payload(response)['days'] == [
        {'on_date': '2026-10-01', 'steps': 0},
        {'on_date': '2026-10-02', 'steps': None},
    ]


def test_daily_steps_exposes_mapping_warnings(monkeypatch):
    def mapper(start_date, end_date, warnings):
        warnings.append({
            'code': 'missing_step_count',
            'message': 'A step record had no count.',
            'on_date': '2026-10-01',
        })
        return []

    monkeypatch.setattr(views, 'get_daily_steps_metrics', mapper)
    response = views.daily_steps(_request('/api/steps/daily', {
        'from': '2026-10-01', 'to': '2026-10-01',
    }))
    assert _payload(response) == {
        'days': [{'on_date': '2026-10-01', 'steps': None}],
        'warnings': [{
            'code': 'missing_step_count',
            'message': 'A step record had no count.',
            'on_date': '2026-10-01',
        }],
    }


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
    def unexpected(*args, **kwargs):
        pytest.fail('Invalid windows must not reach the health provider')

    monkeypatch.setattr(views, 'get_daily_activity_summary', unexpected)
    monkeypatch.setattr(views, 'get_daily_steps_metrics', unexpected)
    assert view(_request(path, query)).status_code == 400


@pytest.mark.parametrize('count', [1, 90])
def test_allowed_window_lengths(monkeypatch, count):
    calls = []
    monkeypatch.setattr(
        views, 'get_daily_steps_metrics',
        lambda start_date, end_date, warnings: calls.append((start_date, end_date)) or [],
    )
    start = date(2026, 1, 1)
    response = views.daily_steps(_request('/api/steps/daily', {
        'from': start.isoformat(),
        'to': (start + timedelta(days=count - 1)).isoformat(),
    }))
    assert response.status_code == 200
    assert len(_payload(response)['days']) == count
    assert calls == [(start, start + timedelta(days=count - 1))]


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
    monkeypatch.setattr(
        views, 'get_daily_steps_metrics',
        lambda start_date, end_date, warnings: (_ for _ in ()).throw(error),
    )
    response = view(_request(path))
    assert response.status_code == status
    assert _payload(response) == {
        'ok': False, 'error': 'Health provider request failed.',
    }


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
    monkeypatch.setattr(
        views, 'get_daily_steps_metrics',
        lambda start_date, end_date, warnings: [],
    )
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
            return {'rollupDataPoints': [{
                'civilStartTime': {'date': {'year': 2026, 'month': 10, 'day': 1}},
                'steps': {'countSum': '1234'},
            }]}
        monkeypatch.setattr(live_api, '_provider_post', post)
    else:
        def get(**kwargs):
            assert kwargs['path'] == 'user/-/activities/steps/date/2026-10-01/2026-10-01.json'
            return {'activities-steps': [{'dateTime': '2026-10-01', 'value': '1234'}]}
        monkeypatch.setattr(live_api, '_provider_get', get)
    response = views.daily_steps(_request('/api/steps/daily', {
        'from': '2026-10-01', 'to': '2026-10-01',
    }))
    assert _payload(response) == {
        'days': [{'on_date': '2026-10-01', 'steps': 1234}],
    }


@pytest.mark.parametrize('provider', ['google', 'fitbit'])
def test_steps_mapping_preserves_zero_and_warns_for_bad_records(monkeypatch, provider):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', provider)
    if provider == 'google':
        def fake_rollup(start_date, end_date):
            return {'rollupDataPoints': [
                {
                    'civilStartTime': {'date': {'year': 2026, 'month': 10, 'day': 1}},
                    'steps': {'countSum': 0},
                },
                {
                    'civilStartTime': {'date': {'year': 2026, 'month': 10, 'day': 2}},
                    'steps': {},
                },
                'malformed row',
                {
                    'civilStartTime': None,
                    'steps': {'countSum': 4},
                },
                {
                    'civilStartTime': {'date': {'year': 2026, 'month': 10, 'day': 2}},
                    'steps': {'countSum': -1},
                },
                {
                    'civilStartTime': {'date': {'year': 2026, 'month': 9, 'day': 30}},
                    'steps': {'countSum': 5},
                },
            ]}
        monkeypatch.setattr(live_api, '_google_daily_steps_rollup', fake_rollup)
    else:
        monkeypatch.setattr(
            live_api, '_provider_get',
            lambda **kwargs: {'activities-steps': [
                {'dateTime': '2026-10-01', 'value': '0'},
                {'dateTime': '2026-10-02'},
                'malformed row',
                {'value': '4'},
                {'dateTime': '2026-10-02', 'value': '-1'},
                {'dateTime': '2026-09-30', 'value': '5'},
            ]},
        )

    warnings = []
    rows = live_api.get_daily_steps_metrics(
        date(2026, 10, 1), date(2026, 10, 2), warnings=warnings,
    )
    assert rows == [{'on_date': '2026-10-01', 'steps': 0}]
    assert {warning['code'] for warning in warnings} == {
        'missing_step_count', 'invalid_step_count', 'step_date_out_of_range',
        'invalid_step_record', 'invalid_step_date',
    }


@pytest.mark.parametrize('provider', ['google', 'fitbit'])
def test_empty_step_response_leaves_dates_unobserved(monkeypatch, provider):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', provider)
    if provider == 'google':
        monkeypatch.setattr(live_api, '_google_daily_steps_rollup', lambda **kwargs: {
            'rollupDataPoints': [],
        })
    else:
        monkeypatch.setattr(live_api, '_provider_get', lambda **kwargs: {
            'activities-steps': [],
        })
    warnings = []
    rows = live_api.get_daily_steps_metrics(
        date(2026, 10, 1), date(2026, 10, 1), warnings=warnings,
    )
    assert rows == []
    assert warnings == []


@pytest.mark.parametrize('provider', ['google', 'fitbit'])
def test_malformed_step_response_warns_instead_of_claiming_coverage(monkeypatch, provider):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', provider)
    if provider == 'google':
        monkeypatch.setattr(live_api, '_google_daily_steps_rollup', lambda **kwargs: {})
    else:
        monkeypatch.setattr(live_api, '_provider_get', lambda **kwargs: {})
    warnings = []
    rows = live_api.get_daily_steps_metrics(
        date(2026, 10, 1), date(2026, 10, 1), warnings=warnings,
    )
    assert rows == []
    assert warnings[0]['code'] == 'invalid_step_response'


def test_duplicate_step_dates_are_unobserved_with_warning(monkeypatch):
    monkeypatch.setenv('FITNICK_HEALTH_PROVIDER', 'google')
    monkeypatch.setattr(live_api, '_google_daily_steps_rollup', lambda **kwargs: {
        'rollupDataPoints': [
            {
                'civilStartTime': {'date': {'year': 2026, 'month': 10, 'day': 1}},
                'steps': {'countSum': 1},
            },
            {
                'civilStartTime': {'date': {'year': 2026, 'month': 10, 'day': 1}},
                'steps': {'countSum': 2},
            },
        ],
    })
    warnings = []
    rows = live_api.get_daily_steps_metrics(
        date(2026, 10, 1), date(2026, 10, 1), warnings=warnings,
    )
    assert rows == []
    assert warnings[0]['code'] == 'duplicate_step_date'


@pytest.mark.parametrize('path,view', ENDPOINTS)
def test_openapi_contract(path, view):
    spec = _payload(views.openapi_spec(_request('/openapi.json')))
    operation = spec['paths'][path]['get']
    assert operation['security'] == [{'ApiKeyAuth': []}]
    assert [p['name'] for p in operation['parameters']] == ['from', 'to']
    content = operation['responses']['200']['content']['application/json']
    schema = content['schema']
    assert 'on_date' in schema['properties']['days']['items']['required']
    if view == views.daily_steps:
        assert schema['properties']['days']['items']['properties']['steps']['type'] == ['integer', 'null']
        assert 'warnings' in schema['properties']
