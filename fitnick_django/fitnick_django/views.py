from datetime import datetime

from django.shortcuts import render
from fitnick.activity.activity import Activity


def index(request):

    activity_api = Activity(config={'base_date': '2021-01-02'}) # replace with real day checks
    response = activity_api.query_daily_activity_summary()
    steps_this_time = response['summary']['steps']
    dt = datetime.now()
    print(steps_this_time, dt)
    index_context = {
        "base_date": activity_api.config['base_date'],
        "steps": steps_this_time,
        "time": str(dt)
    }
    #  get steps
    #  print steps
    #  eventually, print a progress bar re ccurrent steps/daily goal
    # iterate from there!

    return render(request, 'index.html', index_context)
