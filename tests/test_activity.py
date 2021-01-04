import os

import pytest

from fitnick.activity.activity import Activity  # ugly import for now, but there are bigger fish to fry..
from fitnick.activity.models.activity import ActivityLogRecord, activity_log_table, steps_intraday_table
from fitnick.activity.models.calories import Calories, calories_table
from fitnick.database.database import Database
from fitnick.time_series import plot_rolling_average

EXPECTED_DAILY_ACTIVITY_RESPONSE = {'activities': [{'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 91, 'description': '', 'distance': 0.577838, 'duration': 679000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-12-24T16:01:58.000Z', 'logId': 36487726513, 'name': 'Treadmill', 'startDate': '2020-12-24', 'startTime': '10:44', 'steps': 1236}, {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 184, 'description': '', 'distance': 1.178393, 'duration': 1552000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-12-24T17:03:52.000Z', 'logId': 36487795727, 'name': 'Treadmill', 'startDate': '2020-12-24', 'startTime': '11:37', 'steps': 2524}, {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 77, 'description': '', 'distance': 0.492319, 'duration': 608000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-12-24T19:19:52.000Z', 'logId': 36489026309, 'name': 'Treadmill', 'startDate': '2020-12-24', 'startTime': '13:50', 'steps': 1035}, {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 126, 'description': '', 'distance': 0.754637, 'duration': 1099000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-12-25T00:32:21.000Z', 'logId': 36489666573, 'name': 'Treadmill', 'startDate': '2020-12-24', 'startTime': '19:01', 'steps': 1618}, {'activityId': 20049, 'activityParentId': 20049, 'activityParentName': 'Treadmill', 'calories': 255, 'description': '', 'distance': 1.671799, 'duration': 1969000, 'hasActiveZoneMinutes': False, 'hasStartTime': True, 'isFavorite': False, 'lastModified': '2020-12-25T04:01:25.000Z', 'logId': 36489745633, 'name': 'Treadmill', 'startDate': '2020-12-24', 'startTime': '22:17', 'steps': 3502}], 'goals': {'activeMinutes': 70, 'caloriesOut': 3100, 'distance': 9.66, 'floors': 10, 'steps': 12000}, 'summary': {'activeScore': -1, 'activityCalories': 1206, 'caloriesBMR': 1808, 'caloriesOut': 2867, 'distances': [{'activity': 'Treadmill', 'distance': 0.577838}, {'activity': 'Treadmill', 'distance': 1.178393}, {'activity': 'Treadmill', 'distance': 0.492319}, {'activity': 'Treadmill', 'distance': 0.754637}, {'activity': 'Treadmill', 'distance': 1.671799}, {'activity': 'total', 'distance': 5.73}, {'activity': 'tracker', 'distance': 5.73}, {'activity': 'loggedActivities', 'distance': 4.6749860000000005}, {'activity': 'veryActive', 'distance': 3.58}, {'activity': 'moderatelyActive', 'distance': 0.76}, {'activity': 'lightlyActive', 'distance': 1.38}, {'activity': 'sedentaryActive', 'distance': 0}], 'elevation': 120, 'fairlyActiveMinutes': 18, 'floors': 12, 'heartRateZones': [{'caloriesOut': 2482.44676, 'max': 96, 'min': 30, 'minutes': 1372, 'name': 'Out of Range'}, {'caloriesOut': 349.55904, 'max': 134, 'min': 96, 'minutes': 46, 'name': 'Fat Burn'}, {'caloriesOut': 0, 'max': 163, 'min': 134, 'minutes': 0, 'name': 'Cardio'}, {'caloriesOut': 0, 'max': 220, 'min': 163, 'minutes': 0, 'name': 'Peak'}], 'lightlyActiveMinutes': 141, 'marginalCalories': 719, 'restingHeartRate': 64, 'sedentaryMinutes': 703, 'steps': 12053, 'veryActiveMinutes': 68}}
EXPECTED_CALORIE_RESPONSE = {'activeScore': -1, 'activityCalories': 1467, 'caloriesBMR': 1838, 'caloriesOut': 3116, 'distances': [{'activity': 'Treadmill', 'distance': 0.987632}, {'activity': 'Treadmill', 'distance': 0.577689}, {'activity': 'Treadmill', 'distance': 0.040327}, {'activity': 'Treadmill', 'distance': 0.440571}, {'activity': 'Treadmill', 'distance': 2.876452}, {'activity': 'total', 'distance': 6.08}, {'activity': 'tracker', 'distance': 6.08}, {'activity': 'loggedActivities', 'distance': 4.922670999999999}, {'activity': 'veryActive', 'distance': 4.1}, {'activity': 'moderatelyActive', 'distance': 0.77}, {'activity': 'lightlyActive', 'distance': 1.2}, {'activity': 'sedentaryActive', 'distance': 0}], 'elevation': 70, 'fairlyActiveMinutes': 22, 'floors': 7, 'heartRateZones': [{'caloriesOut': 2203.32886, 'max': 96, 'min': 30, 'minutes': 1297, 'name': 'Out of Range'}, {'caloriesOut': 788.48992, 'max': 134, 'min': 96, 'minutes': 104, 'name': 'Fat Burn'}, {'caloriesOut': 80.4321, 'max': 163, 'min': 134, 'minutes': 7, 'name': 'Cardio'}, {'caloriesOut': 0, 'max': 220, 'min': 163, 'minutes': 0, 'name': 'Peak'}], 'lightlyActiveMinutes': 155, 'marginalCalories': 922, 'restingHeartRate': 62, 'sedentaryMinutes': 680, 'steps': 12738, 'veryActiveMinutes': 82}

EXPECTED_DAILY_ACTIVITY_ROWS = [
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=36487726513, calories=91, distance=0.577838, duration=679000, duration_minutes=11.316666666666666, start_date='2020-12-24', start_time='10:44', steps=1236),
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=36487795727, calories=184, distance=1.178393, duration=1552000, duration_minutes=25.866666666666667, start_date='2020-12-24', start_time='11:37', steps=2524),
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=36489026309, calories=77, distance=0.492319, duration=608000, duration_minutes=10.133333333333333, start_date='2020-12-24', start_time='13:50', steps=1035),
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=36489666573, calories=126, distance=0.754637, duration=1099000, duration_minutes=18.316666666666666, start_date='2020-12-24', start_time='19:01', steps=1618),
    ActivityLogRecord(activity_id=20049, activity_name='Treadmill', log_id=36489745633, calories=255, distance=1.671799, duration=1969000, duration_minutes=32.81666666666667, start_date='2020-12-24', start_time='22:17', steps=3502)
]

EXPECTED_COMPARE_CALORIE_ROWS = []


def test_query_daily_activity_summary():
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-12-24'}
    )

    response = activity.query_daily_activity_summary()
    assert response == EXPECTED_DAILY_ACTIVITY_RESPONSE


def test_parse_daily_activity_summary():
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-12-24'}
    )
    rows = activity.parse_activity_log(EXPECTED_DAILY_ACTIVITY_RESPONSE)

    assert rows == EXPECTED_DAILY_ACTIVITY_ROWS


def test_insert_daily_activity_summary():
    database = Database('fitbit_test', 'activity')
    connection = database.engine.connect()

    connection.execute(activity_log_table.delete())
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )
    rows = activity.insert_log_data(database, EXPECTED_DAILY_ACTIVITY_ROWS)
    assert len(rows) == 5


def test_insert_intraday_steps():
    database = Database('fitbit_test', 'activity')
    connection = database.engine.connect()

    connection.execute(steps_intraday_table.delete())
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )
    rows = activity.insert_log_data(database, EXPECTED_DAILY_ACTIVITY_ROWS)
    assert len(rows) == 5


def test_query_calorie_summary():
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )
    response = activity.query_calorie_summary()
    assert response == EXPECTED_CALORIE_RESPONSE


def test_insert_calorie_data():
    database = Database('fitbit_test', 'activity')
    connection = database.engine.connect()

    connection.execute(calories_table.delete())
    activity = Activity(
        config={'database': 'fitbit_test',
                'base_date': '2020-10-01'}
    )
    raw_data = activity.query_calorie_summary()
    row = activity.parse_calorie_summary('2020-10-01', raw_data)
    inserted_row = activity.insert_calorie_data(database, row)

    assert inserted_row == Calories(date='2020-10-01', total=3116, calories_bmr=1838, activity_calories=1467)


def test_backfill_calories():
    database = Database('fitbit_test', 'activity')
    connection = database.engine.connect()
    activity = Activity(
        config={'database': 'fitbit_test'}
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


@pytest.mark.skip(reason='takes inordinately long - test locally.')
def test_insert_steps_intraday():
    activity = Activity(
        config={'database': 'fitbit_test',
                'table': 'calories',
                'sum_column': 'total',
                'base_date': '2020-12-26'
                }
    )
    database = Database('fitbit_test', 'activity')
    response = activity.insert_steps_intraday(database)
