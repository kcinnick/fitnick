import os
import json
from datetime import datetime

from django.http import JsonResponse
from django.shortcuts import render

from fitnick.base.live_api import (
    HealthAPIError,
    HealthConfigurationError,
    MAX_DAILY_HEART_RANGE_DAYS,
    get_identity_summary,
    get_latest_body_fat_entry,
    get_latest_sleep_session,
    get_recent_steps,
    get_daily_heart_rate_metrics,
    get_health_provider,
    get_daily_activity_summary,
    run_smoke_test,
    uses_live_health_api,
)
from fitnick import __version__


DEFAULT_STEPS_GOAL = 12000


def _settings_file_path():
    configured = os.getenv('FITNICK_SETTINGS_FILE', '').strip()
    if configured:
        return configured
    return '/var/data/fitnick/settings.json'


def _load_user_settings():
    path = _settings_file_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
            return payload if isinstance(payload, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_user_settings(settings_payload):
    path = _settings_file_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(settings_payload, f, indent=2, sort_keys=True)


def _coerce_positive_int(value):
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _configured_default_steps_goal():
    return _coerce_positive_int(os.getenv('FITNICK_DEFAULT_STEPS_GOAL', str(DEFAULT_STEPS_GOAL))) or DEFAULT_STEPS_GOAL


def _goal_from_activity_response(response):
    if not isinstance(response, dict):
        return None
    goals = response.get('goals', {})
    if isinstance(goals, dict):
        return _coerce_positive_int(goals.get('steps'))
    return None


def _get_steps_goal_override():
    return _coerce_positive_int(_load_user_settings().get('steps_goal_override'))


def _set_steps_goal_override(goal_value):
    settings_payload = _load_user_settings()
    settings_payload['steps_goal_override'] = int(goal_value)
    _save_user_settings(settings_payload)


def _clear_steps_goal_override():
    settings_payload = _load_user_settings()
    if 'steps_goal_override' in settings_payload:
        del settings_payload['steps_goal_override']
    _save_user_settings(settings_payload)


def _resolve_steps_goal(activity_response=None):
    override_goal = _get_steps_goal_override()
    if override_goal:
        return override_goal
    provider_goal = _goal_from_activity_response(activity_response)
    if provider_goal:
        return provider_goal
    return _configured_default_steps_goal()


def _is_scope_permission_error(exc):
    if not isinstance(exc, HealthAPIError):
        return False
    error_type = str(getattr(exc, 'error_type', '')).lower()
    return exc.status_code == 403 and ('permission' in error_type or 'scope' in str(exc).lower())
def index(request):
    goal = _configured_default_steps_goal()
    today = datetime.today().strftime('%Y-%m-%d')
    errors = []
    steps_this_time = 0
    daily_activity_response = None
    identity = None
    recent_steps = []
    latest_sleep = None
    latest_body_fat = None

    if uses_live_health_api():
        try:
            daily_activity_response = get_daily_activity_summary(today)
            steps_this_time = int(daily_activity_response.get('summary', {}).get('steps', 0))
        except (HealthAPIError, HealthConfigurationError) as exc:
            errors.append(str(exc))

        goal = _resolve_steps_goal(daily_activity_response)

        try:
            identity = get_identity_summary()
        except HealthAPIError as exc:
            if _is_scope_permission_error(exc):
                pass  # Silently skip identity if scope is missing
            else:
                errors.append(str(exc))
        except HealthConfigurationError as exc:
            errors.append(str(exc))

        try:
            recent_steps = get_recent_steps(days=7)
        except (HealthAPIError, HealthConfigurationError) as exc:
            errors.append(str(exc))

        try:
            latest_sleep = get_latest_sleep_session()
        except (HealthAPIError, HealthConfigurationError) as exc:
            errors.append(str(exc))

        try:
            latest_body_fat = get_latest_body_fat_entry()
        except (HealthAPIError, HealthConfigurationError) as exc:
            errors.append(str(exc))

    dt = datetime.now()
    percent = (steps_this_time / goal) * 100 if goal else 0
    index_context = {
        "base_date": today,
        "today": True,
        "steps": steps_this_time,
        "time": str(dt),
        "goal": goal,
        "percent": percent,
        "percent_str": str(percent)[:6],
        "errors": errors,
        "service_mode": 'live' if uses_live_health_api() else 'offline',
        "provider": get_health_provider(),
        "identity": identity,
        "recent_steps": recent_steps,
        "latest_sleep": latest_sleep,
        "latest_body_fat": latest_body_fat,
    }

    return render(request, 'index.html', index_context)


def get_steps_today(request):
    status_code = 200
    error = None

    goal = _configured_default_steps_goal()
    today = datetime.today().strftime('%Y-%m-%d')
    try:
        response = get_daily_activity_summary(today)
        steps_this_time = int(response.get('summary', {}).get('steps', 0))
        goal = _resolve_steps_goal(response)
    except (HealthAPIError, HealthConfigurationError) as exc:
        steps_this_time = 0
        error = str(exc)
        status_code = getattr(exc, 'status_code', 500)

    dt = datetime.now()
    percent = (steps_this_time / goal) * 100 if goal else 0
    index_context = {
        "base_date": today,
        "today": True,
        "steps": steps_this_time,
        "time": str(dt),
        "goal": goal,
        "percent": percent,
        "percent_str": str(percent)[:6],
        "errors": [error] if error else [],
        "service_mode": 'live' if uses_live_health_api() else 'offline',
        "provider": get_health_provider(),
        "identity": None,
        "recent_steps": [],
        "latest_sleep": None,
        "latest_body_fat": None,
    }

    return render(request, 'index.html', index_context, status=status_code)


def healthcheck(request):
    return JsonResponse({
        'ok': True,
        'status': 'ok',
        'service': 'fitnick',
        'version': __version__,
        'git_sha': os.getenv('RENDER_GIT_COMMIT') or os.getenv('GIT_SHA'),
        'provider': get_health_provider(),
        'health_api_mode': 'live' if uses_live_health_api() else 'offline',
    })


def openapi_spec(request):
    return JsonResponse(
        {
            'openapi': '3.0.3',
            'info': {
                'title': 'fitnick service API',
                'version': __version__,
            },
            'paths': {
                '/health': {
                    'get': {
                        'summary': 'Service health',
                        'responses': {
                            '200': {
                                'description': 'Service is healthy',
                            }
                        },
                    }
                },
                '/api/heart-rate/daily': {
                    'get': {
                        'summary': 'Daily heart-rate metrics in a date window',
                        'security': [{'ApiKeyAuth': []}],
                        'parameters': [
                            {
                                'in': 'query',
                                'name': 'from',
                                'required': True,
                                'schema': {'type': 'string', 'format': 'date'},
                            },
                            {
                                'in': 'query',
                                'name': 'to',
                                'required': True,
                                'schema': {'type': 'string', 'format': 'date'},
                            },
                        ],
                        'responses': {
                            '200': {
                                'description': 'Daily heart metrics',
                            },
                            '400': {'description': 'Invalid query params'},
                            '401': {'description': 'Unauthorized'},
                        },
                    }
                },
            },
            'components': {
                'securitySchemes': {
                    'ApiKeyAuth': {
                        'type': 'apiKey',
                        'in': 'header',
                        'name': 'X-API-Key',
                    }
                }
            },
        }
    )


def settings_page(request):
    message = ''
    error = ''
    today = datetime.today().strftime('%Y-%m-%d')
    provider_goal = None
    provider_goal_error = ''

    if request.method == 'POST':
        action = request.POST.get('action', 'save')
        if action == 'clear_override':
            _clear_steps_goal_override()
            message = 'Goal override cleared. Using provider/default goal now.'
        else:
            parsed_goal = _coerce_positive_int(request.POST.get('steps_goal'))
            if parsed_goal is None:
                error = 'Please enter a positive whole number for the daily steps goal.'
            else:
                _set_steps_goal_override(parsed_goal)
                message = 'Goal override saved.'

    if uses_live_health_api():
        try:
            provider_response = get_daily_activity_summary(today)
            provider_goal = _goal_from_activity_response(provider_response)
        except (HealthAPIError, HealthConfigurationError) as exc:
            provider_goal_error = str(exc)

    override_goal = _get_steps_goal_override()
    effective_goal = override_goal or provider_goal or _configured_default_steps_goal()

    return render(
        request,
        'settings.html',
        {
            'provider': get_health_provider(),
            'service_mode': 'live' if uses_live_health_api() else 'offline',
            'override_goal': override_goal,
            'provider_goal': provider_goal,
            'provider_goal_error': provider_goal_error,
            'effective_goal': effective_goal,
            'default_goal': _configured_default_steps_goal(),
            'message': message,
            'error': error,
        },
    )


def daily_heart_rate(request):
    from_raw = request.GET.get('from')
    to_raw = request.GET.get('to')
    if not from_raw or not to_raw:
        return JsonResponse(
            {'ok': False, 'error': 'Query params "from" and "to" are required (YYYY-MM-DD).'},
            status=400,
        )

    try:
        from_date = datetime.strptime(from_raw, '%Y-%m-%d').date()
        to_date = datetime.strptime(to_raw, '%Y-%m-%d').date()
    except ValueError:
        return JsonResponse(
            {'ok': False, 'error': 'Invalid date format. Use YYYY-MM-DD for "from" and "to".'},
            status=400,
        )

    if from_date > to_date:
        return JsonResponse({'ok': False, 'error': '"from" must be on or before "to".'}, status=400)

    if (to_date - from_date).days + 1 > MAX_DAILY_HEART_RANGE_DAYS:
        return JsonResponse(
            {
                'ok': False,
                'error': f'Date range cannot exceed {MAX_DAILY_HEART_RANGE_DAYS} inclusive days.',
            },
            status=400,
        )

    try:
        days = get_daily_heart_rate_metrics(start_date=from_date, end_date=to_date)
        return JsonResponse({'days': days})
    except (HealthAPIError, HealthConfigurationError) as exc:
        return JsonResponse({'ok': False, 'error': str(exc)}, status=getattr(exc, 'status_code', 503))


def health_smoke_test(request):
    try:
        payload = run_smoke_test()
        return JsonResponse(payload)
    except (HealthAPIError, HealthConfigurationError) as exc:
        return JsonResponse({
            'ok': False,
            'provider': get_health_provider(),
            'mode': 'live' if uses_live_health_api() else 'offline',
            'refresh_configured': False,
            'error': str(exc),
        }, status=getattr(exc, 'status_code', 500))


def fitbit_smoke_test(request):
    return health_smoke_test(request)
