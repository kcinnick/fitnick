from datetime import datetime

from django.shortcuts import render
from fitnick.activity.activity import Activity


def index(request):
    activity_api = Activity(config={'base_date': '2021-01-02'})
    response = activity_api.query_daily_activity_summary()
    steps_this_time = response['summary']['steps']
    dt = datetime.now()
    print(steps_this_time, dt)
    index_context = {
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
    index_context = {
        "base_date": activity_api.config['base_date'],
        "steps": steps_this_time,
        "time": str(dt),
        "goal": goal
    }

    return render(request, 'index.html', index_context)
