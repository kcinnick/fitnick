#!/usr/bin/env python

"""Tests for the `heart_rate` functions in fitnick."""

import datetime
import os

from decimal import Decimal

import pytest

from fitnick.base.base import create_db_engine, get_authorized_client
from fitnick.heart_rate.heart_rate import insert_heart_rate_time_series_data, query_heart_rate_zone_time_series, \
    parse_response

from fitnick.heart_rate.models import heart_daily_table

EXPECTED_ROWS = [
    ('Out of Range', Decimal('1267.00000'), datetime.date(2020, 9, 5), Decimal('2086.83184'), 68),
    ('Fat Burn', Decimal('115.00000'), datetime.date(2020, 9, 5), Decimal('721.58848'), 68),
    ('Cardio', Decimal('3.00000'), datetime.date(2020, 9, 5), Decimal('30.91792'), 68),
    ('Peak', Decimal('0.00000'), datetime.date(2020, 9, 5), Decimal('0.00000'), 68)
]

EXPECTED_DATA = {'activities-heart': [
    {'dateTime': '2020-09-05', 'value': {'customHeartRateZones': [], 'heartRateZones': [
        {'caloriesOut': 2086.83184, 'max': 96, 'min': 30, 'minutes': 1267, 'name': 'Out of Range'},
        {'caloriesOut': 721.58848, 'max': 134, 'min': 96, 'minutes': 115, 'name': 'Fat Burn'},
        {'caloriesOut': 30.91792, 'max': 163, 'min': 134, 'minutes': 3, 'name': 'Cardio'},
        {'caloriesOut': 0, 'max': 220, 'min': 163, 'minutes': 0, 'name': 'Peak'}
    ], 'restingHeartRate': 68}}]}


def purge(db_connection, delete_sql_string, select_sql_string):
    """
    Deletes & asserts that records from tables were deleted to test certain functions are working.
    :return:
    """
    # deleting the test rows
    db_connection.execute(delete_sql_string)

    rows = [i for i in db_connection.execute(select_sql_string)]
    assert len(rows) == 0


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_get_heart_rate_time_series_period():
    db_connection = create_db_engine(database='fitbit_test')

    db_connection.execute(heart_daily_table.delete())

    insert_heart_rate_time_series_data(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1d'})

    rows = [row for row in db_connection.execute('select * from heart.daily')]

    assert rows == EXPECTED_ROWS


def test_query_heart_rate_zone_time_series():
    data = query_heart_rate_zone_time_series(
        authorized_client=get_authorized_client(),
        config={
            'database': 'fitbit_test',
            'base_date': '2020-09-05',
            'end_date': '2020-09-05'
        }
    )

    assert data == EXPECTED_DATA


def test_parse_response():
    rows = parse_response(EXPECTED_DATA)
    for index, row in enumerate(rows):
        assert row.type == EXPECTED_ROWS[index][0]
        assert row.minutes == EXPECTED_ROWS[index][1]
        assert row.date == EXPECTED_ROWS[index][2].strftime('%Y-%m-%d')
        assert row.calories == float(EXPECTED_ROWS[index][3])
        assert row.resting_heart_rate == EXPECTED_ROWS[index][4]
