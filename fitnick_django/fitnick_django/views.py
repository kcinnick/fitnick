from datetime import datetime

from django.http import JsonResponse
from django.shortcuts import render

from fitnick.base.live_api import (
    HealthAPIError,
    HealthConfigurationError,
    get_health_provider,
    get_daily_activity_summary,
    run_smoke_test,
    uses_live_health_api,
)


def index(request):
    return render(request, 'index.html', {
        'service_mode': 'live' if uses_live_health_api() else 'offline',
        'provider': get_health_provider(),
    })


def get_steps_today(request):
    goal = 12000  # set automatically, eventually..
    today = datetime.today().strftime('%Y-%m-%d')
    status_code = 200
    error = None

    try:
        response = get_daily_activity_summary(today)
        steps_this_time = response['summary']['steps']
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
        "error": error,
        "service_mode": 'live' if uses_live_health_api() else 'offline',
        "provider": get_health_provider(),
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
