from datetime import datetime

from django.http import JsonResponse
from django.shortcuts import render

from fitnick.base.live_api import (
    HealthAPIError,
    HealthConfigurationError,
    get_identity_summary,
    get_latest_body_fat_entry,
    get_latest_sleep_session,
    get_recent_steps,
    get_health_provider,
    get_daily_activity_summary,
    run_smoke_test,
    uses_live_health_api,
)


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
        except (HealthAPIError, HealthConfigurationError) as exc:
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
        'status': 'ok',
        'service': 'fitnick',
        'provider': get_health_provider(),
        'health_api_mode': 'live' if uses_live_health_api() else 'offline',
    })


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
