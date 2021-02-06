import os
from datetime import datetime, timedelta

import pandas as pd
from fitnick.base.base import create_spark_session, get_df_from_db
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from fitnick.heart_rate.models import heart_daily_table

from pyspark.sql import functions as F


def get_calories_for_month(database='fitbit'):
    # aggregate sum calories for a certain month
    db_connection = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
        f":5432/{database}", poolclass=NullPool
    )
    df = pd.read_sql_table(
        con=db_connection,
        table_name='calories',
        schema='activity'
    )
    df = df.groupby(df.date.dt.month).agg(['sum'])
    print(df)

    return df


def compare_1d_heart_rate_zone_data(heart_rate_zone, date_str, table=heart_daily_table, database='fitbit'):
    """
    Retrieves & compares today & yesterday's heart rate zone data for the zone specified.
    :param database: Database to compare data fromm.
    :param heart_rate_zone: str, Heart rate zone data desired. Options are Cardio, Peak, Fat Burn & Out of Range.
    :param date_str: str, representing the date to search for.
    :param table: sqlalchemy.Table object to retrieve data from.
    :return:
    """
    db_connection = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
        f":5432/{database}", poolclass=NullPool
    )
    search_datetime = datetime.strptime(date_str, '%Y-%m-%d')
    previous_date_string = (search_datetime - timedelta(days=1)).date()

    with db_connection.connect() as connection:
        day_row = connection.execute(
            table.select().where(
                table.columns.date == str(search_datetime.date())
            ).where(table.columns.type == heart_rate_zone)
        ).fetchone()

        previous_day_row = connection.execute(
            table.select().where(table.columns.date == str(previous_date_string)
                                 ).where(table.columns.type == heart_rate_zone)
        ).fetchone()

    print(
        f"You spent {day_row.minutes} minutes in {heart_rate_zone} today, compared to " +
        f"{previous_day_row.minutes} yesterday."
    )

    if heart_rate_zone != 'Out of Range':
        if day_row.minutes < previous_day_row.minutes:
            print(
                f'Get moving! That\'s {previous_day_row.minutes - day_row.minutes} minutes less than yesterday!'
            )
        else:
            print('Good work! That\'s {} minutes more than yesterday!'.format(
                int(day_row.minutes - previous_day_row.minutes)
            ))

    return heart_rate_zone, day_row.minutes, previous_day_row.minutes


def compare_steps_by_day(database='fitbit'):
    spark_session = create_spark_session()
    df = get_df_from_db(
        spark_session=spark_session, database=database, schema='activity', table='steps_intraday')
    today_df = df.where(F.col('date') == '2021-01-03')
    yesterday_df = df.where(F.col('date') == '2021-01-06')

    print(today_df.groupBy().sum().collect())
    print(yesterday_df.groupBy().sum().collect())


def main():
    # get_calories_for_month()
    compare_steps_by_day()


if __name__ == '__main__':
    main()
