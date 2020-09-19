from datetime import timedelta, datetime
import os

from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from fitnick.heart_rate.models import heart_daily_table


def build_sql_expression(table, conditions):
    expression = table.select().where(
        table.columns.date == conditions[0]).where(
        table.columns.type == conditions[1]
    )
    return expression


class Database:
    def __init__(self, database: str):
        self.engine = create_engine(
            f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
            f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
            f":5432/{database}", poolclass=NullPool
        )
        self.spark_session = None

        return

    def create_spark_session(self):
        """
        We may not always want a spark session when interacting with the database - i.e.,
        simple inserts do not require one. Larger scale analysis does, however, and those
        types of situations are where this method would be called.
        :return:
        """
        if self.spark_session:
            return

        self.spark_session = SparkSession.builder.getOrCreate()
        return

    def get_df_from_db(self, database, schema, table):
        """
        Retrieves a PySpark dataframe containing all of the data in the specified table.
        :param database: str, name of database
        :param schema: str, name of schema
        :param table: str, name of table
        :return: DataFrame
        """
        properties = {
            "driver": "org.postgresql.Driver",
            "user": os.environ['POSTGRES_USERNAME'],
            "password": os.environ['POSTGRES_PASSWORD'],
            "currentSchema": schema
        }

        df = self.spark_session.read.jdbc(
            url=f"jdbc:postgresql://{os.environ['POSTGRES_IP']}/{database}",
            properties=properties,
            table=table
        )

        return df

    def compare_1d_heart_rate_zone_data(self, heart_rate_zone, date_str, table=heart_daily_table):
        """
        Retrieves & compares today & yesterday's heart rate zone data for the zone specified.
        :param heart_rate_zone: str, Heart rate zone data desired. Options are Cardio, Peak, Fat Burn & Out of Range.
        :param date_str: str, representing the date to search for.
        :param table: sqlalchemy.Table object to retrieve data from.
        :return:
        """
        search_datetime = datetime.strptime(date_str, '%Y-%m-%d')
        previous_date_string = (search_datetime - timedelta(days=1)).date()

        with self.engine.connect() as connection:
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
