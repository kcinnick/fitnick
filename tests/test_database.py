import decimal
from datetime import date

from fitnick.base.base import get_authorized_client, create_db_engine
from fitnick.database.database import compare_1d_heart_rate_zone_data
from fitnick.heart_rate.heart_rate import get_heart_rate_zone_time_series
from fitnick.models import heart_daily_table


def test_compare_1d_heart_rate_zone_data():
    db_connection = create_db_engine(database='fitbit_test')
    db_connection.execute(heart_daily_table.delete().where(heart_daily_table.columns.date == date.today().strftime('%Y-%m-%d')))

    get_heart_rate_zone_time_series(
        authorized_client=get_authorized_client(),
        database='fitbit_test',
        table=heart_daily_table,
        config={
            'base_date': date.today().strftime('%Y-%m-%d'),
            'period': '1d'
        }
    )
    heart_rate_zone, minutes_in_zone_today, minutes_in_zone_yesterday = compare_1d_heart_rate_zone_data(
        database='fitbit_test', table=heart_daily_table
    )
    assert (heart_rate_zone, type(minutes_in_zone_today), type(minutes_in_zone_yesterday)) == (
        'Cardio', decimal.Decimal, decimal.Decimal)
