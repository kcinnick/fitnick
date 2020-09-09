import decimal
import os
from datetime import date, timedelta

import pytest

from fitnick.base.base import create_spark_session, create_db_engine, get_df_from_db
from fitnick.database.database import compare_1d_heart_rate_zone_data
from fitnick.heart_rate.heart_rate import insert_heart_rate_time_series_data
from fitnick.heart_rate.models import heart_daily_table


@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_compare_1d_heart_rate_zone_data():
    today = date.today()
    yesterday = today - timedelta(days=1)

    insert_heart_rate_time_series_data(
        config={
            'end_date': today.strftime('%Y-%m-%d'),
            'base_date': yesterday.strftime('%Y-%m-%d'),
            'database': 'fitbit_test'
        }
    )

    spark = create_spark_session()

    heart_rate_zone, minutes_in_zone_today, minutes_in_zone_yesterday = compare_1d_heart_rate_zone_data(
        spark_session=spark,
        database='fitbit_test',
        schema='heart',
        heart_rate_zone='Cardio'
    )

    assert (heart_rate_zone, type(minutes_in_zone_today), type(minutes_in_zone_yesterday)) == (
        'Cardio', decimal.Decimal, decimal.Decimal)

    spark.stop()


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
