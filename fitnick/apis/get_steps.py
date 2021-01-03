from datetime import date, timedelta
from time import sleep

from fitnick.database.database import Database
from tqdm import tqdm

from fitnick.activity.activity import Activity
from datetime import datetime

from win10toast import ToastNotifier

toaster = ToastNotifier()


def get_steps_for_day():
    steps_last_time = 0
    steps_this_time = 0

    sdate = date(2020, 1, 1)  # start date
    edate = date(2021, 1, 2)  # end date

    delta = edate - sdate  # as timedelta
    activity_api = Activity(config={'base_date': ''})

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        activity_api.config['base_date'] =  day
        response = activity_api.insert_steps_intraday()

        steps_this_time = response['summary']['steps']
        dt = datetime.now()

        with open('steps.csv', 'a') as f:
            f.write(f"{steps_this_time},{dt}\n")

        # Show notification whenever needed
        toaster.show_toast(
            "Fitnick Alert",
            f"You have taken {steps_this_time} steps so far today! Keep it up!",
            threaded=False,
            icon_path=None,
            duration=3  # 3 seconds
        )

        print(str(steps_this_time), str(steps_last_time))

        toaster.show_toast(
            "Fitnick Alert",
            f"You took {steps_this_time - steps_last_time} steps since my last alert!",
            threaded=False,
            icon_path=None,
            duration=5
        )

        steps_last_time = steps_this_time
        for _ in tqdm(range(0, 600)):
            sleep(1)

    return int(steps_this_time)


def batch_load_steps(start_date='2020-01-01', end_date='2021-01-01'):
    steps_last_time = 0
    steps_this_time = 0

    sdate = date(2020, 1, 1)  # start date
    edate = date(2020, 11, 18)  # end date

    delta = edate - sdate  # as timedelta
    activity_api = Activity(config={'base_date': ''})

    database = Database('fitbit', 'activity')
    for i in tqdm(list(reversed(range(delta.days + 1)))):
        day = sdate + timedelta(days=i)
        activity_api.config['base_date'] = day
        response = activity_api.insert_steps_intraday(database)


def check_if_steps_need_update(base_date='2021-01-01'):
    activity_api = Activity(config={'base_date': base_date})
    response = activity_api.query_daily_activity_summary()
    steps_total = response['summary']['steps']
    #  get steps in db

    database = Database('fitbit', 'activity')
    connection = database.engine.connect()
    response = connection.execute(
        "select * from activity.steps_intraday where date = '{}'".format(base_date)
    ).fetchall()
    steps_intraday = 0
    for result in response:
        steps_intraday += result[2]

    if steps_total != steps_intraday:
        match = False
        msg = '\nSteps total does not equal steps intraday.\nNeed to rerun get_steps for {}'.format(base_date)
        comparison = '\nSteps total: {}\nSteps intraday: {}'.format(steps_total, steps_intraday)
        print(msg)
        print(comparison)
    else:
        match = True
        msg = '\nSteps total matches steps intraday.'
        print(msg)

    return match


def main():
    batch_load_steps()


if __name__ == '__main__':
    main()
