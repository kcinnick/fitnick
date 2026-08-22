from datetime import datetime

from django.http import JsonResponse
from django.shortcuts import render

from fitnick.base.live_api import (
    FitbitAPIError,
    FitbitConfigurationError,
    get_daily_activity_summary,
    run_smoke_test,
    uses_live_fitbit_api,
)


def index(request):
    return render(request, 'index.html', {'service_mode': 'live' if uses_live_fitbit_api() else 'offline'})


def get_steps_today(request):
    goal = 12000  # set automatically, eventually..
    today = datetime.today().strftime('%Y-%m-%d')
    status_code = 200
    error = None

    try:
        response = get_daily_activity_summary(today)
        steps_this_time = response['summary']['steps']
    except (FitbitAPIError, FitbitConfigurationError) as exc:
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
        "service_mode": 'live' if uses_live_fitbit_api() else 'offline',
    }

    return render(request, 'index.html', index_context, status=status_code)


def healthcheck(request):
    return JsonResponse({
        'status': 'ok',
        'service': 'fitnick',
        'fitbit_mode': 'live' if uses_live_fitbit_api() else 'offline',
    })


def fitbit_smoke_test(request):
    try:
        payload = run_smoke_test()
        return JsonResponse(payload)
    except (FitbitAPIError, FitbitConfigurationError) as exc:
        return JsonResponse({
            'ok': False,
            'mode': 'live' if uses_live_fitbit_api() else 'offline',
            'refresh_configured': False,
            'error': str(exc),
        }, status=getattr(exc, 'status_code', 500))
