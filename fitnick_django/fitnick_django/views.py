from datetime import datetime

from django.shortcuts import render
from fitnick.activity.activity import Activity


def index(request):
    today_date = datetime.today().strftime('%Y-%m-%d')
    activity_api = Activity(config={'base_date': today_date})
    response = activity_api.query_daily_activity_summary()
    steps_this_time = response['summary']['steps']
    dt = datetime.now()
    print(steps_this_time, dt)
    index_context = {
        "today": True,
        "base_date": activity_api.config['base_date'],
        "steps": steps_this_time,
        "time": str(dt)
    }

    return render(request, 'index.html', index_context)


def get_steps_today(request):
    goal = 12000  # set automatically, eventually..
    today = datetime.today().strftime('%Y-%m-%d')
    activity_api = Activity(config={'base_date': today})
    response = activity_api.query_daily_activity_summary()
    steps_this_time = response['summary']['steps']
    dt = datetime.now()
    print(steps_this_time, dt)
    # TODO:
    # show a table of last x days of steps
    index_context = {
        "base_date": activity_api.config['base_date'],
        "today": True,
        "steps": steps_this_time,
        "time": str(dt),
        "goal": goal
    }

    return render(request, 'index.html', index_context)
