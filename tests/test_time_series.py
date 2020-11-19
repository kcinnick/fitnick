import pytest

from fitnick.activity.models.calories import calories_table
from fitnick.time_series import TimeSeries


def test_validate_inputs():
    time_series = TimeSeries(
        config={'base_date': '2020-10-01',
                'end_date': '2020-10-02'}
    )
    assert time_series.validate_input()

    with pytest.raises(AttributeError):
        TimeSeries(
            config={'base_date': '10-1-2020',
                    'end_date': '10-02-2020'}
        ).validate_input()


def test_backfill():
    time_series = TimeSeries(
        config={'base_date': '2020-10-01',
                'end_date': '2020-10-02',
                'database': 'fitbit_test',
                'schema': 'activity',
                'table': calories_table,
                'resource': 'calories'})

    time_series.backfill(90)
