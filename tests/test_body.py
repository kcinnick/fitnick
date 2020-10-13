import datetime
import os
from decimal import Decimal

import pytest

from fitnick.body.body import WeightTimeSeries, BodyFat
from fitnick.body.models.bodyfat import BodyFatRecord, bodyfat_table
from fitnick.database.database import Database

EXPECTED_WEIGHT_RESPONSE = {'body-weight': [{'dateTime': '2020-09-05', 'value': '176.0'}]}

EXPECTED_WEIGHT_DATA = [(datetime.date(2020, 9, 5), Decimal('176.0')), (datetime.date(2020, 9, 6), Decimal('175.2'))]

EXPECTED_BODYFAT_RESPONSE = {
    'fat': [
        {'date': '2020-09-05', 'fat': 18, 'logId': 1599350399000, 'source': 'API', 'time': '23:59:59'},
        {'date': '2020-09-11', 'fat': 18, 'logId': 1599868799000, 'source': 'API', 'time': '23:59:59'},
        {'date': '2020-09-18', 'fat': 18, 'logId': 1600473599000, 'source': 'API', 'time': '23:59:59'},
        {'date': '2020-09-22', 'fat': 18, 'logId': 1600819199000, 'source': 'API', 'time': '23:59:59'},
        {'date': '2020-09-23', 'fat': 18, 'logId': 1600905599000, 'source': 'API', 'time': '23:59:59'},
        {'date': '2020-09-27', 'fat': 18, 'logId': 1601251199000, 'source': 'API', 'time': '23:59:59'},
        {'date': '2020-09-30', 'fat': 18, 'logId': 1601510399000, 'source': 'API', 'time': '23:59:59'}]
}

EXPECTED_BODYFAT_DATA = [
    BodyFatRecord(date='2020-09-05', fat=18, source='API', time='23:59:59'),
    BodyFatRecord(date='2020-09-11', fat=18, source='API', time='23:59:59'),
    BodyFatRecord(date='2020-09-18', fat=18, source='API', time='23:59:59'),
    BodyFatRecord(date='2020-09-22', fat=18, source='API', time='23:59:59'),
    BodyFatRecord(date='2020-09-23', fat=18, source='API', time='23:59:59'),
    BodyFatRecord(date='2020-09-27', fat=18, source='API', time='23:59:59'),
    BodyFatRecord(date='2020-09-30', fat=18, source='API', time='23:59:59'),
]

EXPECTED_BODYFAT_RECORD = BodyFatRecord(date='2019-01-01', fat=18, source='API', time='23:59:59')


def test_query_body_weight_time_series():
    data = WeightTimeSeries(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'end_date': '2020-09-05',
        'resource': 'weight'}
    ).query()

    assert data == EXPECTED_WEIGHT_RESPONSE


def test_parse_weight_response():
    row = WeightTimeSeries(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1d'}
    ).parse_response(EXPECTED_WEIGHT_RESPONSE)[0]
    assert row.date == '2020-09-05' and row.pounds == '176.0'


def test_insert_weight_data():
    from fitnick.body.models.weight import weight_table

    database = Database('fitbit_test', 'weight')
    connection = database.engine.connect()

    connection.execute(weight_table.delete())

    WeightTimeSeries(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1d'}).insert_data()

    rows = [row for row in connection.execute(weight_table.select())]
    connection.close()

    assert sorted(rows) == sorted(EXPECTED_WEIGHT_DATA)


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_body_plot():
    # although there's no assertions, this effectively tests the plotting method for weight
    # via the config.
    WeightTimeSeries(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1d',
        'table': 'daily'}).plot()


def test_get_body_fat():
    response = BodyFat(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1m',
        'table': 'daily'}).query()

    assert response == EXPECTED_BODYFAT_RESPONSE


def test_parse_bodyfat_response():
    parsed_response = BodyFat(config={}).parse_response(EXPECTED_BODYFAT_RESPONSE)

    assert parsed_response == EXPECTED_BODYFAT_DATA


def test_insert_bodyfat():
    """
    Deletes & re-inserts data, then asserts the data was inserted as expected.
    :return:
    """
    database = Database('fitbit_test', 'bodyfat')
    connection = database.engine.connect()

    connection.execute(bodyfat_table.delete())

    BodyFat(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1d',
        'table': 'daily'}).insert_data()

    rows = [row for row in connection.execute(bodyfat_table.select())]
    assert len(rows) == 1

    
def test_get_bodyfat():
    body_fat = BodyFat(config={})
    body_fat.log('2019-01-01', '18.0')

    body_fat = BodyFat(config={
        'database': 'fitbit_test',
        'base_date': '2019-01-01',
        'end_date': '2019-01-02'
    })
    rows = body_fat.get_bodyfat_rows()

    assert rows == [EXPECTED_BODYFAT_RECORD]


def test_log_bodyfat():
    body_fat = BodyFat(config={})
    response = body_fat.log('2019-01-01', '18.0')
    assert (response['fat'], response['time'], response['date']) == (18, '23:59:59', '2019-01-01')


def test_delete_bodyfat():
    body_fat = BodyFat(config={})
    response = body_fat.delete('1546387199000')
    assert response  # if the log was deleted, response will be True
