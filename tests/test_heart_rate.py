#!/usr/bin/env python

"""Tests for the `heart_rate` functions in fitnick."""

import datetime
import os

from decimal import Decimal

import pytest

from fitnick.database.database import Database
from fitnick.heart_rate.models import heart_daily_table
from fitnick.heart_rate.heart_rate import HeartRateTimeSeries

EXPECTED_ROWS = [
    ('Out of Range', Decimal('1267.00000'), datetime.date(2020, 9, 5), Decimal('2086.83184'), 68),
    ('Fat Burn', Decimal('115.00000'), datetime.date(2020, 9, 5), Decimal('721.58848'), 68),
    ('Cardio', Decimal('3.00000'), datetime.date(2020, 9, 5), Decimal('30.91792'), 68),
    ('Peak', Decimal('0.00000'), datetime.date(2020, 9, 5), Decimal('0.00000'), 68)
]

EXPECTED_PERIOD_ROWS = [
    ('Out of Range', Decimal('1267.00000'), datetime.date(2020, 9, 5), Decimal('2086.83184'), 68),
    ('Cardio', Decimal('3.00000'), datetime.date(2020, 9, 5), Decimal('30.91792'), 68),
    ('Out of Range', Decimal('1210.00000'), datetime.date(2020, 9, 6), Decimal('1967.73432'), 69),
    ('Cardio', Decimal('2.00000'), datetime.date(2020, 9, 6), Decimal('24.22690'), 69),
    ('Fat Burn', Decimal('115.00000'), datetime.date(2020, 9, 5), Decimal('721.58848'), 68),
    ('Peak', Decimal('0.00000'), datetime.date(2020, 9, 5), Decimal('0.00000'), 68),
    ('Fat Burn', Decimal('178.00000'), datetime.date(2020, 9, 6), Decimal('950.84207'), 69),
    ('Peak', Decimal('0.00000'), datetime.date(2020, 9, 6), Decimal('0.00000'), 69)
]

EXPECTED_DATA = {'activities-heart': [
    {'dateTime': '2020-09-05', 'value': {'customHeartRateZones': [], 'heartRateZones': [
        {'caloriesOut': 2086.83184, 'max': 96, 'min': 30, 'minutes': 1267, 'name': 'Out of Range'},
        {'caloriesOut': 721.58848, 'max': 134, 'min': 96, 'minutes': 115, 'name': 'Fat Burn'},
        {'caloriesOut': 30.91792, 'max': 163, 'min': 134, 'minutes': 3, 'name': 'Cardio'},
        {'caloriesOut': 0, 'max': 220, 'min': 163, 'minutes': 0, 'name': 'Peak'}
    ], 'restingHeartRate': 68}}]}


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_get_heart_rate_time_series_period():
    database = Database('fitbit_test', 'heart')
    connection = database.engine.connect()

    connection.execute(heart_daily_table.delete())

    HeartRateTimeSeries(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1d'}
    ).insert_data()

    rows = [row for row in connection.execute(heart_daily_table.select())]
    connection.close()

    assert sorted(rows) == sorted(EXPECTED_PERIOD_ROWS)


def test_query_heart_rate_zone_time_series():
    data = HeartRateTimeSeries(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'end_date': '2020-09-05'}
    ).query()

    data.pop('activities-heart-intraday') # quickfix for not yet parsing intraday heart data

    assert data == EXPECTED_DATA


def test_parse_response():
    rows = HeartRateTimeSeries(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1d'}
    ).parse_response(EXPECTED_DATA)

    for index, row in enumerate(rows):
        assert row.type == EXPECTED_ROWS[index][0]
        assert row.minutes == EXPECTED_ROWS[index][1]
        assert row.date == EXPECTED_ROWS[index][2].strftime('%Y-%m-%d')
        assert row.calories == float(EXPECTED_ROWS[index][3])
        assert row.resting_heart_rate == EXPECTED_ROWS[index][4]


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_get_total_calories():
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    df = heart_rate_zone.get_total_calories_df(show=False)

    testing_date = (df.where(df.date == '2020-08-08').withColumnRenamed('sum(calories)', 'sum_calories')).take(1)[0]
    assert testing_date.sum_calories == Decimal('3063.96122')
