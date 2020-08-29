#!/usr/bin/env python

"""Tests for `fitnick` package."""

from datetime import datetime
import pytest
import os

import fitbit
from sqlalchemy import create_engine

from fitnick import fitnick


@pytest.mark.skip(reason="fails on GCP because of Travis-CI issues, passes locally.")
def test_get_authed_client():
    """Tests that authorization is working as expected."""
    assert type(fitnick.get_authorized_client()) == fitbit.Fitbit


def test_get_heart_rate_time_series_period():
    db_connection = create_engine(f"postgres+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}:5432/fitbit_test")
    authed_client = fitnick.get_authorized_client()
    delete_sql_string = f"delete from heart.daily where date='2020-08-26'"
    select_sql_string = f"select * from heart.daily where date='2020-08-26'"
    with db_connection.connect() as connection:
        # deleting the test rows
        connection.execute(delete_sql_string)
        rows = [i for i in connection.execute(select_sql_string)]
        assert len(rows) == 0

    fitnick.get_heart_rate_time_series_period(authed_client, db_connection=db_connection, date='2020-08-26', period='1d')
    with db_connection.connect() as connection:
        # re-adding them
        rows = [i for i in connection.execute(select_sql_string)]
        assert len(rows) == 4


def test_get_heart_rate_time_series_daterange():
    db_connection = create_engine(f"postgres+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}:5432/fitbit_test")
    authed_client = fitnick.get_authorized_client()
    delete_sql_string = f"delete from heart.daterange where base_date='2020-08-26' and end_date='2020-08-27'"
    select_sql_string = f"select * from heart.daterange where base_date='2020-08-26' and end_date='2020-08-27'"
    with db_connection.connect() as connection:
        # deleting the test rows
        connection.execute(delete_sql_string)
        rows = [i for i in connection.execute(select_sql_string)]
        assert len(rows) == 0

    fitnick.get_heart_rate_time_series_daterange(
        authed_client, db_connection=db_connection, base_date='2020-08-26', end_date='2020-08-27')
    with db_connection.connect() as connection:
        # re-adding them
        rows = [i for i in connection.execute(select_sql_string)]
        assert len(rows) == 4


def test_check_date():
    test_date_xpass = '08-26-2020'
    assert fitnick.check_date(test_date_xpass)
    test_date_xfail = datetime.today().strftime('%Y-%m-%d')
    assert not fitnick.check_date(test_date_xfail)
