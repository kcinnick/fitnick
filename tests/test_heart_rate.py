#!/usr/bin/env python

"""Tests for the `heart_rate` functions in fitnick."""

import datetime
from decimal import Decimal

from fitnick.base.base import create_db_engine, get_authorized_client
from fitnick.heart_rate.heart_rate import get_heart_rate_zone_time_series
from fitnick.heart_rate.models import heart_daily_table

HEART_DATERANGE_EXPECTED_ROWS = [
    ('Out of Range', datetime.date(2020, 8, 20), datetime.date(2020, 8, 27), Decimal('1299.00000'),
     Decimal('2164.34791')),
    ('Fat Burn', datetime.date(2020, 8, 20), datetime.date(2020, 8, 27), Decimal('126.00000'), Decimal('819.35015')),
    ('Cardio', datetime.date(2020, 8, 20), datetime.date(2020, 8, 27), Decimal('2.00000'), Decimal('21.40238')),
    ('Peak', datetime.date(2020, 8, 20), datetime.date(2020, 8, 27), Decimal('0.00000'), Decimal('0.00000'))
]

HEART_PERIOD_EXPECTED_ROWS = [
    ('Out of Range', Decimal('1297.00000'), datetime.date(2020, 8, 26), Decimal('2444.62820')),
    ('Fat Burn', Decimal('110.00000'), datetime.date(2020, 8, 26), Decimal('814.44840')),
    ('Cardio', Decimal('0.00000'), datetime.date(2020, 8, 26), Decimal('0.00000')),
    ('Peak', Decimal('0.00000'), datetime.date(2020, 8, 26), Decimal('0.00000'))
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
    db_connection = create_db_engine(database='fitbit_test')
    authorized_client = get_authorized_client()

    period_data = get_heart_rate_zone_time_series(
        authorized_client,
        config={'base_date': '2020-09-02',
                'period': '1d'},
        engine=db_connection
    )

    base_and_end_date_data = get_heart_rate_zone_time_series(
        authorized_client,
        engine=db_connection,
        config={'base_date': '2020-09-02',
                'end_date': '2020-09-02'},
    )

    assert period_data == base_and_end_date_data  # asserting that we get the same response from both invocations
