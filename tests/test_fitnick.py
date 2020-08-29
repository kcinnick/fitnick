#!/usr/bin/env python

"""Tests for `fitnick` package."""

from datetime import datetime
import pytest
import os

import fitbit
from sqlalchemy import create_engine

from fitnick import fitnick


def test_get_authed_client():
    """Tests that authorization is working as expected."""
    assert type(fitnick.get_authed_client()) == fitbit.Fitbit


@pytest.mark.skip(reason="fails on GCP because of Travis-CI issues, passes locally.")
def test_get_heart_rate_time_series_period():
    db_connection = create_engine(f"postgres+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}:5432/fitbit_test")
    authed_client = fitnick.get_authed_client()
    fitnick.get_heart_rate_time_series_period(authed_client, db_connection=db_connection, date='2020-08-26', period='1d')
    sql_string = f"select * from heart.daily where date='2020-08-26'"
    with db_connection.connect() as connection:
        rows = [i for i in connection.execute(sql_string)]
        assert len(rows) == 4


def test_check_date():
    test_date_xpass = '08-26-2020'
    assert fitnick.check_date(test_date_xpass)
    test_date_xfail = datetime.today().strftime('%Y-%m-%d')
    assert not fitnick.check_date(test_date_xfail)
