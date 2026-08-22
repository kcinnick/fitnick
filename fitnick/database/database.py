import os
from datetime import timedelta, datetime

from pyspark.sql import SparkSession
from sqlalchemy import create_engine, MetaData
from sqlalchemy.pool import NullPool, StaticPool

from fitnick.activity.models.activity import meta as activity_meta
from fitnick.body.models.bodyfat import meta as bodyfat_meta
from fitnick.body.models.weight import meta as weight_meta
from fitnick.heart_rate.models import meta as heart_meta, heart_daily_table
from fitnick.sleep.models import meta as sleep_meta


def build_sql_expression(table, conditions):
    expression = table.select().where(
        table.columns.date == conditions[0]).where(
        table.columns.type == conditions[1]
    )
    return expression


def _configure_sqlite_metadata():
    for metadata in (activity_meta, bodyfat_meta, weight_meta, heart_meta, sleep_meta):
        for table in metadata.tables.values():
            table.info['fitnick_original_schema'] = table.schema
            table.schema = None


class Database:
    _engines = {}

    def __init__(self, database: str, schema: str):
        self.database = database
        self.schema = schema
        cache_key = (database, schema)

        if cache_key in self._engines:
            self.engine = self._engines[cache_key]
        else:
            postgres_credentials = (
                os.getenv('POSTGRES_USERNAME') and os.getenv('POSTGRES_PASSWORD') and os.getenv('POSTGRES_IP')
            )
            if postgres_credentials:
                self.engine = create_engine(
                    f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
                    f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
                    f":5432/{database}", poolclass=NullPool
                )
            else:
                _configure_sqlite_metadata()
                self.engine = create_engine(
                    'sqlite:///:memory:',
                    poolclass=StaticPool,
                    connect_args={'check_same_thread': False}
                )
                self._create_sqlite_tables()
            self._engines[cache_key] = self.engine

        self.spark_session = None

        return

    def _create_sqlite_tables(self):
        target_schema = self.schema
        for parent_meta in (
            __import__('fitnick.activity.models.activity', fromlist=['meta']).meta,
            __import__('fitnick.activity.models.calories', fromlist=['meta']).meta,
            __import__('fitnick.body.models.bodyfat', fromlist=['meta']).meta,
            __import__('fitnick.body.models.weight', fromlist=['meta']).meta,
            __import__('fitnick.heart_rate.models', fromlist=['meta']).meta,
            __import__('fitnick.sleep.models', fromlist=['meta']).meta,
        ):
            for table in parent_meta.tables.values():
                original_schema = table.info.get('fitnick_original_schema', table.schema)
                if original_schema not in (None, target_schema):
                    continue
                table.schema = None
                table.create(bind=self.engine, checkfirst=True)

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
