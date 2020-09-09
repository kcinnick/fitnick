import decimal
import os
from datetime import date

import pytest
from sqlalchemy.exc import IntegrityError

from fitnick.base.base import create_db_engine
from fitnick.database.database import compare_1d_heart_rate_zone_data
from fitnick.heart_rate.heart_rate import insert_heart_rate_time_series_data, get_heart_rate_zone_for_day
from fitnick.heart_rate.models import heart_daily_table


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_compare_1d_heart_rate_zone_data():
    try:
        get_heart_rate_zone_for_day(database='fitbit_test', date='2020-09-04')
    except IntegrityError:
        # for the purpose of this test, we don't care if the data is already there for 2020-09-04
        pass

    heart_rate_zone, minutes_in_zone_today, minutes_in_zone_yesterday = compare_1d_heart_rate_zone_data(
        database='fitbit_test', table=heart_daily_table, heart_rate_zone='Cardio', date_str='2020-09-05')

    assert (heart_rate_zone, type(minutes_in_zone_today), type(minutes_in_zone_yesterday)) == (
        'Cardio', decimal.Decimal, decimal.Decimal)


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_check_for_duplicates():
    """
    Useful for asserting that there aren't duplicates in the database, which *should* be avoided in the code.
    """
    db_connection = create_db_engine(database='fitbit_test')

    insert_heart_rate_time_series_data(
        config={
            'base_date': date.today().strftime('2020-09-02'),
            'period': '1d',
            'database': 'fitbit_test'}
    )

    results = db_connection.execute(
        heart_daily_table.select().where(heart_daily_table.columns.date ==
                                         date.today().strftime('2020-09-02'))
    )

    assert len([result for result in results]) == 4
