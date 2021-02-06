from datetime import datetime

from django.shortcuts import render
from fitnick.activity.activity import Activity


def index(request):
    return render(request, 'base.html')


def get_steps_today(request):
    goal = 12000  # set automatically, eventually..
    today = datetime.today().strftime('%Y-%m-%d')
    activity_api = Activity(config={'base_date': today})
    response = activity_api.query_daily_activity_summary()
    steps_this_time = response['summary']['steps']
    dt = datetime.now()
    percent = (steps_this_time / 12000) * 100
    # TODO:
    # show a table of last x days of steps
    index_context = {
        "base_date": activity_api.config['base_date'],
        "today": True,
        "steps": steps_this_time,
        "time": str(dt),
        "goal": goal,
        "percent": percent,
        "percent_str": str(percent)[:6]
    }

    return render(request, 'index.html', index_context)

