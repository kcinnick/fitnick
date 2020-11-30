import os

import pytest

from fitnick.database.database import Database
from fitnick.heart_rate.models import heart_daily_table
from fitnick.heart_rate.time_series import HeartRateTimeSeries


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_check_for_duplicates():
    """
    Useful for asserting that there aren't duplicates in the database, which *should* be avoided in the code.
    """
    database = Database('fitbit_test', 'heart')
    connection = database.engine.connect()

    HeartRateTimeSeries(config={
        'base_date': '2020-09-02',
        'period': '1d',
        'database': 'fitbit_test'}
    ).insert_data(database, heart_daily_table)

    results = connection.execute(
        heart_daily_table.select().where(heart_daily_table.columns.date == '2020-09-02'))

    assert len([result for result in results]) == 4
    connection.close()
