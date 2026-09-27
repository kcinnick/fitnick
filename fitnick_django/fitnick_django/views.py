import os
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


def index(request):
    goal = 12000  # set automatically, eventually..
    today = datetime.today().strftime('%Y-%m-%d')
    errors = []
    steps_this_time = 0
    identity = None
    recent_steps = []
    latest_sleep = None
    latest_body_fat = None

    if uses_live_health_api():
        try:
            response = get_daily_activity_summary(today)
            steps_this_time = int(response.get('summary', {}).get('steps', 0))
        except (HealthAPIError, HealthConfigurationError) as exc:
            errors.append(str(exc))

        try:
            identity = get_identity_summary()
        except HealthAPIError as exc:
            # 403 permission_denied for identity usually means missing profile scope
            # Log but don't display error if it's just a scope issue
            if exc.status_code == 403 and 'permission' in exc.error_type.lower():
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

    goal = 12000
    today = datetime.today().strftime('%Y-%m-%d')
    try:
        response = get_daily_activity_summary(today)
        steps_this_time = int(response.get('summary', {}).get('steps', 0))
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
