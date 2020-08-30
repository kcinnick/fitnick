#!/usr/bin/env python

"""Tests for `fitnick` package."""

import datetime
from decimal import Decimal

import pytest
import os

import fitbit
from sqlalchemy import create_engine

from fitnick import fitnick

HEART_DATERANGE_EXPECTED_ROWS = [
            ('Out of Range', datetime.date(2020, 8, 20), datetime.date(2020, 8, 27), Decimal('1299.00000'), Decimal('2164.34791')),
            ('Fat Burn', datetime.date(2020, 8, 20), datetime.date(2020, 8, 27), Decimal('126.00000'), Decimal('819.35015')),
            ('Cardio', datetime.date(2020, 8, 20), datetime.date(2020, 8, 27), Decimal('2.00000'), Decimal('21.40238')),
            ('Peak', datetime.date(2020, 8, 20), datetime.date(2020, 8, 27), Decimal('0.00000'), Decimal('0.00000'))
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


def test_get_authorized_client():
    assert type(fitnick.get_authorized_client()) == fitbit.Fitbit


def test_get_heart_rate_time_series_period():
    db_connection = create_engine(f"postgres+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}:5432/fitbit_test")
    authorized_client = fitnick.get_authorized_client()
    delete_sql_string = f"delete from heart.daily where date='2020-08-26'"
    select_sql_string = f"select * from heart.daily where date='2020-08-26'"

    purge(db_connection, delete_sql_string, select_sql_string)

    fitnick.get_heart_rate_time_series_period(authorized_client, db_connection=db_connection, date='2020-08-26', period='1d')
    with db_connection.connect() as connection:
        # re-adding them
        rows = [i for i in connection.execute(select_sql_string)]
        assert len(rows) == 4


def test_get_heart_rate_time_series_daterange():
    db_connection = create_engine(f"postgres+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}:5432/fitbit_test")
    authorized_client = fitnick.get_authorized_client()

    base_date = '2020-08-20'
    end_date = '2020-08-27'

    delete_sql_string = f"delete from heart.daterange where base_date='{base_date}' and end_date='{end_date}'"
    select_sql_string = f"select * from heart.daterange where base_date='{base_date}' and end_date='{end_date}'"

    with db_connection.connect() as connection:
        purge(connection, delete_sql_string, select_sql_string)

        fitnick.get_heart_rate_time_series(
            authorized_client, db_connection=connection, config={
                'database': 'heart',
                'table': 'daterange',
                'base_date': base_date,
                'end_date': end_date,
                'columns': ['base_date', 'end_date', 'type', 'minutes', 'calories']}
        )

        # checking that they were re-added
    with db_connection.connect() as connection:
        rows = [i for i in connection.execute(select_sql_string)]
        assert sorted(rows) == sorted(HEART_DATERANGE_EXPECTED_ROWS)


def test_check_date():
    test_date_xpass = '08-26-2020'
    assert fitnick.check_date(test_date_xpass)
    test_date_xfail = datetime.datetime.today().strftime('%Y-%m-%d')
    assert not fitnick.check_date(test_date_xfail)
