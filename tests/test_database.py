import decimal
from datetime import date, timedelta

from fitnick.base.base import get_authorized_client, create_db_engine
from fitnick.database.database import compare_1d_heart_rate_zone_data
from fitnick.heart_rate.heart_rate import insert_heart_rate_time_series_data
from fitnick.heart_rate.models import heart_daily_table


def test_compare_1d_heart_rate_zone_data():
    db_connection = create_db_engine(database='fitbit_test')

    today = date.today()
    yesterday = today - timedelta(days=1)
    db_connection.execute(
        heart_daily_table.delete().where(heart_daily_table.columns.date == today.strftime('%Y-%m-%d')))

    insert_heart_rate_time_series_data(
        config={
            'base_date': today.strftime('%Y-%m-%d'),
            'end_date': yesterday.strftime('%Y-%m-%d'),
            'database': 'fitbit_test'
        }
    )
    heart_rate_zone, minutes_in_zone_today, minutes_in_zone_yesterday = compare_1d_heart_rate_zone_data(
        database='fitbit_test', table=heart_daily_table)

    assert (heart_rate_zone, type(minutes_in_zone_today), type(minutes_in_zone_yesterday)) == (
        'Cardio', decimal.Decimal, decimal.Decimal)


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
