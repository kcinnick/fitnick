import datetime
from decimal import Decimal

from fitnick.body.body import WeightTimeSeries
from fitnick.database.database import Database

EXPECTED_WEIGHT_RESPONSE = {'body-weight': [{'dateTime': '2020-09-05', 'value': '176.0'}]}

EXPECTED_WEIGHT_DATA = [(datetime.date(2020, 9, 5), Decimal('176.0')), (datetime.date(2020, 9, 6), Decimal('175.2'))]


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
    from fitnick.body.models import weight_table

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


def test_body_plot():
    # although there's no assertions, this effectively tests the plotting method for weight
    # via the config.
    WeightTimeSeries(config={
        'database': 'fitbit_test',
        'base_date': '2020-09-05',
        'period': '1d',
        'table': 'daily'}).plot()
