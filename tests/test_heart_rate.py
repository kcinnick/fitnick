#!/usr/bin/env python

"""Tests for the `heart_rate` functions in fitnick."""

import datetime
from decimal import Decimal

from fitnick.base.base import create_db_engine, get_authorized_client
from fitnick.heart_rate.heart_rate import insert_heart_rate_time_series_data, query_heart_rate_zone_time_series

EXPECTED_ROWS = [
    ('Out of Range', Decimal('1297.00000'), datetime.date(2020, 8, 26), Decimal('2444.62820'), 65),
    ('Fat Burn', Decimal('110.00000'), datetime.date(2020, 8, 26), Decimal('814.44840'), 65),
    ('Cardio', Decimal('0.00000'), datetime.date(2020, 8, 26), Decimal('0.00000'), 65),
    ('Peak', Decimal('0.00000'), datetime.date(2020, 8, 26), Decimal('0.00000'), 65)
]

EXPECTED_DATA = {'activities-heart': [{'dateTime': '2020-09-06', 'value': {'customHeartRateZones': [],
                                                                           'heartRateZones': [
                                                                               {'caloriesOut': 1418.136, 'max': 96,
                                                                                'min': 30, 'minutes': 944,
                                                                                'name': 'Out of Range'},
                                                                               {'caloriesOut': 259.09728, 'max': 134,
                                                                                'min': 96, 'minutes': 40,
                                                                                'name': 'Fat Burn'},
                                                                               {'caloriesOut': 0, 'max': 163,
                                                                                'min': 134, 'minutes': 0,
                                                                                'name': 'Cardio'},
                                                                               {'caloriesOut': 0, 'max': 220,
                                                                                'min': 163, 'minutes': 0,
                                                                                'name': 'Peak'}],
                                                                           'restingHeartRate': 69}}]}


def purge(db_connection, delete_sql_string, select_sql_string):
    """
    Deletes & asserts that records from tables were deleted to test certain functions are working.
    :return:
    """
    # deleting the test rows
    db_connection.execute(delete_sql_string)

    rows = [i for i in db_connection.execute(select_sql_string)]
    assert len(rows) == 0


def test_get_heart_rate_time_series_period():
    db_connection = create_db_engine(database='fitbit_test', schema='heart')

    purge(db_connection, 'delete from daily;', 'select * from daily')

    insert_heart_rate_time_series_data(config={
        'database': 'fitbit_test',
        'base_date': '2020-08-26',
        'period': '1d'
    })

    rows = [row for row in db_connection.execute('select * from daily')]

    assert rows == EXPECTED_ROWS


def test_query_heart_rate_zone_time_series():
    data = query_heart_rate_zone_time_series(
        authorized_client=get_authorized_client(),
        config={
            'database': 'fitbit_test',
            'base_date': datetime.date.today().strftime('%Y-%m-%d'),
            'period': '1d'
        }
    )
    assert data == EXPECTED_DATA
