import pytest

from fitnick.body.body import WeightTimeSeries

EXPECTED_WEIGHT_RESPONSE = {'body-weight': [{'dateTime': '2020-09-05', 'value': '176.0'}]}
EXPECTED_WEIGHT_ROWS = ()


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
