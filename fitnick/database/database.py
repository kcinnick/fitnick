import os
from datetime import timedelta, datetime

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
    def __init__(self, database: str, schema: str):
        self.database = database
        self.schema = schema
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

    def get_df_from_db(self, table):
        """
        Retrieves a PySpark dataframe containing all of the data in the specified table.
        :param table: str, name of table
        :return: DataFrame
        """
        properties = {
            "driver": "org.postgresql.Driver",
            "user": os.environ['POSTGRES_USERNAME'],
            "password": os.environ['POSTGRES_PASSWORD'],
            "currentSchema": self.schema
        }

        df = self.spark_session.read.jdbc(
            url=f"jdbc:postgresql://{os.environ['POSTGRES_IP']}/{self.database}",
            properties=properties,
            table=table
        )

        return df
