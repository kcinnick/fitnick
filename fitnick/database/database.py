import os
from datetime import timedelta
from datetime import date

from fitnick.base.base import create_db_engine, create_spark_session
from fitnick.heart_rate.models import heart_daily_table


def build_sql_expression(table, conditions):
    expression = table.select().where(
        table.columns.date == conditions[0]).where(
        table.columns.type == conditions[1]
    )
    return expression


def compare_1d_heart_rate_zone_data(spark_session, heart_rate_zone='Cardio', database='fitbit', schema="heart"):
    """
    Retrieves & compares today & yesterday's heart rate zone data for the zone specified.
    :param heart_rate_zone: str, Heart rate zone data desired. Options are Cardio, Peak, Fat Burn & Out of Range.
    :param database: str, Database to use for data comparison. Options are fitbit or fitbit-test.
    :param table: sqlalchemy.Table object to retrieve data from.
    :return:
    """
    properties = {
        "driver": "org.postgresql.Driver",
        "user": os.environ['POSTGRES_USERNAME'],
        "password": os.environ['POSTGRES_PASSWORD'],
        "currentSchema": schema
    }

    today_date_string = date.today()
    yesterday_date_string = date.today() - timedelta(days=1)

    df = spark_session.read.jdbc(
        url=f"jdbc:postgresql://{os.environ['POSTGRES_IP']}/{database}",
        properties=properties,
        table='daily'
    )

    today_df, yesterday_df = df.where(df.date == today_date_string), df.where(df.date == yesterday_date_string)

    minutes_in_zone_today = today_df.where(df.type == heart_rate_zone).take(1)[0].minutes
    minutes_in_zone_yesterday = yesterday_df.where(df.type == heart_rate_zone).take(1)[0].minutes

    print(
        f"You spent {minutes_in_zone_today} minutes in {heart_rate_zone} today, compared to " +
        f"{minutes_in_zone_yesterday} yesterday."
    )

    if heart_rate_zone != 'Out of Range':
        if minutes_in_zone_today < minutes_in_zone_yesterday:
            print('Get moving! That\'s {} minutes less than yesterday!'.format(
                int(minutes_in_zone_yesterday - minutes_in_zone_today)
            ))
        else:
            print('Good work! That\'s {} minutes more than yesterday!'.format(
                int(minutes_in_zone_today - minutes_in_zone_yesterday)
            ))

    return heart_rate_zone, minutes_in_zone_today, minutes_in_zone_yesterday
