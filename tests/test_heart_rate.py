#!/usr/bin/env python

"""Tests for the `heart_rate` functions in fitnick."""

import datetime
from decimal import Decimal

from fitnick.base.base import create_db_engine
from fitnick.heart_rate.heart_rate import insert_heart_rate_time_series_data

EXPECTED_ROWS = [
    ('Out of Range', Decimal('1297.00000'), datetime.date(2020, 8, 26), Decimal('2444.62820'), 65),
    ('Fat Burn', Decimal('110.00000'), datetime.date(2020, 8, 26), Decimal('814.44840'), 65),
    ('Cardio', Decimal('0.00000'), datetime.date(2020, 8, 26), Decimal('0.00000'), 65),
    ('Peak', Decimal('0.00000'), datetime.date(2020, 8, 26), Decimal('0.00000'), 65)
]


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
