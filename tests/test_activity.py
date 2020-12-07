import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from fitnick.activity.activity import Activity  # ugly import for now, but there are bigger fish to fry..
from fitnick.activity.models.activity import ActivityLogRecord, activity_log_table
from fitnick.activity.models.calories import Calories, calories_table
from fitnick.time_series import plot_rolling_average

EXPECTED_DAILY_ACTIVITY_RESPONSE = {
    'activities': [
        {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 170, 'description': '', 'distance': 0.987632, 'duration': 1233000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-10-01T04:31:23.000Z', 'logId': 34686180447, 'name': 'Treadmill', 'startDate': '2020-10-01', 'startTime': '00:10', 'steps': 2030},
        {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 99, 'description': '', 'distance': 0.577689, 'duration': 767000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-10-01T18:10:22.000Z', 'logId': 34685023369, 'name': 'Treadmill', 'startDate': '2020-10-01', 'startTime': '13:55', 'steps': 1218},
        {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 59, 'description': '', 'distance': 0.040327, 'duration': 2124000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-10-01T21:21:59.000Z', 'logId': 34687543940, 'name': 'Treadmill', 'startDate': '2020-10-01', 'startTime': '16:22', 'steps': 114},
        {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 104, 'description': '', 'distance': 0.440571, 'duration': 815000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-10-01T23:15:56.000Z', 'logId': 34684730042, 'name': 'Treadmill', 'startDate': '2020-10-01', 'startTime': '19:01', 'steps': 977},
        {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 481, 'description': '', 'distance': 2.876452, 'duration': 3402000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-10-02T03:07:46.000Z', 'logId': 34687447116, 'name': 'Treadmill', 'startDate': '2020-10-01', 'startTime': '22:10', 'steps': 6012}],
    'goals': {'activeMinutes': 70, 'caloriesOut': 3000, 'distance': 8.05, 'floors': 10, 'steps': 11000},
    'summary': {'activeScore': -1, 'activityCalories': 1467, 'caloriesBMR': 1838, 'caloriesOut': 3116, 'distances': [
        {'activity': 'Treadmill', 'distance': 0.987632},
        {'activity': 'Treadmill', 'distance': 0.577689},
        {'activity': 'Treadmill', 'distance': 0.040327},
        {'activity': 'Treadmill', 'distance': 0.440571},
        {'activity': 'Treadmill', 'distance': 2.876452},
        {'activity': 'total', 'distance': 6.08},
        {'activity': 'tracker', 'distance': 6.08},
        {'activity': 'loggedActivities', 'distance': 4.922670999999999},
        {'activity': 'veryActive', 'distance': 4.1},
        {'activity': 'moderatelyActive', 'distance': 0.77},
        {'activity': 'lightlyActive', 'distance': 1.2},
        {'activity': 'sedentaryActive', 'distance': 0}
    ],
        'elevation': 70,
        'fairlyActiveMinutes': 22,
        'floors': 7,
        'heartRateZones': [
            {'caloriesOut': 2203.32886, 'max': 96, 'min': 30, 'minutes': 1297, 'name': 'Out of Range'},
            {'caloriesOut': 788.48992, 'max': 134, 'min': 96, 'minutes': 104, 'name': 'Fat Burn'},
            {'caloriesOut': 80.4321, 'max': 163, 'min': 134, 'minutes': 7, 'name': 'Cardio'},
            {'caloriesOut': 0, 'max': 220, 'min': 163, 'minutes': 0, 'name': 'Peak'}
        ],
        'lightlyActiveMinutes': 155,
        'marginalCalories': 922,
        'restingHeartRate': 62,
        'sedentaryMinutes': 680,
        'steps': 12738,
        'veryActiveMinutes': 82}
}

EXPECTED_DAILY_ACTIVITY_ROWS = [
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=34686180447, calories=170, distance=0.987632, duration=1233000, duration_minutes=20.55, start_date='2020-10-01', start_time='00:10', steps=2030),
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=34685023369, calories=99, distance=0.577689, duration=767000, duration_minutes=12.783333333333333, start_date='2020-10-01', start_time='13:55', steps=1218),
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=34687543940, calories=59, distance=0.040327, duration=2124000, duration_minutes=35.4, start_date='2020-10-01', start_time='16:22', steps=114),
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=34684730042, calories=104, distance=0.440571, duration=815000, duration_minutes=13.583333333333334, start_date='2020-10-01', start_time='19:01', steps=977),
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=34687447116, calories=481, distance=2.876452, duration=3402000, duration_minutes=56.7, start_date='2020-10-01', start_time='22:10', steps=6012)
]

EXPECTED_COMPARE_CALORIE_ROWS = []


def test_query_daily_activity_summary():
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )

    response = activity.query_daily_activity_summary()

    assert response == EXPECTED_DAILY_ACTIVITY_RESPONSE


def test_parse_daily_activity_summary():
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )
    rows = activity.parse_activity_log(EXPECTED_DAILY_ACTIVITY_RESPONSE)

    assert rows == EXPECTED_DAILY_ACTIVITY_ROWS


def test_insert_daily_activity_summary():
    engine = create_engine(
            f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
            f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
            f":5432/fitbit_test", poolclass=NullPool
        )
    connection = engine.connect()

    connection.execute(activity_log_table.delete())
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )
    rows = activity.insert_log_data(connection, EXPECTED_DAILY_ACTIVITY_ROWS)
    assert len(rows) == 5


def test_query_calorie_summary():
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )
    response = activity.query_calorie_summary()

    assert response == EXPECTED_DAILY_ACTIVITY_RESPONSE['summary']


def test_insert_calorie_data():
    engine = create_engine(
            f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
            f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
            f":5432/fitbit_test", poolclass=NullPool
        )

    connection = engine.connect()
    connection.execute(calories_table.delete())

    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )
    raw_data = activity.query_calorie_summary()
    row = activity.parse_calorie_summary('2020-10-01', raw_data)
    inserted_row = activity.insert_calorie_data(engine, row)

    assert inserted_row == Calories(date='2020-10-01', total=3116, calories_bmr=1838, activity_calories=1467)


def test_backfill_calories():
    database = 'fitbit_test'

    engine = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
        f":5432/{database}", poolclass=NullPool
    )

    connection = engine.connect()
    activity = Activity(
        config={'database': database}
    )

    connection.execute(calories_table.delete())
    rows = [i for i in connection.execute(calories_table.select())]
    assert len(rows) == 0

    activity.backfill_calories(3)
    assert len([i for i in connection.execute(calories_table.select())]) == 3


def test_get_lifetime_stats():
    activity = Activity(
        config={'database': 'fitbit_test'}
    )
    lifetime_stats, best_stats = activity.get_lifetime_stats()

    assert list(lifetime_stats.keys()) == ['distance', 'floors', 'steps']
    assert list(best_stats.keys()) == ['distance', 'floors', 'steps']


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_compare_calories_across_week():
    activity = Activity(
        config={'database': 'fitbit'}
    )

    rows = activity.compare_calories_across_week('2020-10-11', 6)
    assert rows == (19985, 19196)


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_plot_rolling_average():
    activity = Activity(
        config={'database': 'fitbit',
                'table': 'calories',
                'sum_column': 'total',
                'base_date': '2020-10-01',
                'end_date': '2020-10-29'}
    )
    plot_rolling_average(activity.config)
