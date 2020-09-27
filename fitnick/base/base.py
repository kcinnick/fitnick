"""Main module."""
import os
from datetime import datetime, timedelta, date

import fitbit
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import FlushError
from tqdm import tqdm

from fitnick.database.database import Database
from fitnick.heart_rate.models import heart_daily_table


def handle_integrity_error(session, row):
    session.rollback()
    insert_statement = insert(heart_daily_table).values(
        type=row.type,
        minutes=row.minutes,
        date=row.date,
        calories=row.calories,
        resting_heart_rate=row.resting_heart_rate)

    update_statement = insert_statement.on_conflict_do_update(
        constraint='daily_type_date_key',
        set_={
            'type': row.type,
            'minutes': row.minutes,
            'date': row.date,
            'calories': row.calories,
            'resting_heart_rate': row.resting_heart_rate
        })

    session.execute(update_statement)
    session.commit()

    return session


def get_authorized_client() -> fitbit.Fitbit:
    """
    Using the defined environment variables for the various Fitbit tokens,
    creates an authorized Fitbit client for a user's credentials.
    :return: Authorized Fitbit client
    """
    authorized_client = fitbit.Fitbit(
        os.environ['FITBIT_CONSUMER_KEY'],
        os.environ['FITBIT_CONSUMER_SECRET'],
        os.environ['FITBIT_ACCESS_KEY'],
        os.environ['FITBIT_REFRESH_TOKEN']
    )

    if authorized_client.sleep:
        #  If the client isn't authorized, this method will return NoneType
        return authorized_client
    else:
        print(
            'Authorization failed - check your refresh token, and ensure'
            + 'your environment variables are set correctly.')
        exit()


def refresh_authorized_client():
    import requests
    with requests.session() as session:
        data = {'grant_type': 'refresh_token',
                'refresh_token': os.environ['FITBIT_REFRESH_TOKEN']}
        r = session.post(
            url='https://api.fitbit.com/oauth2/token',
            data=data,
            headers={
                'clientId': os.environ['FITBIT_CONSUMER_KEY'],
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f"Basic {os.environ['FITBIT_AUTH_HEADER']}"}
        )
        os.environ['FITBIT_ACCESS_KEY'] = r.json()['access_token']
        os.environ['FITBIT_REFRESH_TOKEN'] = r.json()['refresh_token']
        print(r.json())

    return


def get_df_from_db(spark_session, database: str, schema: str, table: str):
    """
    Retrieves a PySpark dataframe containing all of the data in the specified table.
    :param spark_session: Existing SparkSession object
    :param database: str, name of database
    :param schema: str, name of database schema
    :param table: str, name of table
    :return: DataFrame
    """
    properties = {
        "driver": "org.postgresql.Driver",
        "user": os.environ['POSTGRES_USERNAME'],
        "password": os.environ['POSTGRES_PASSWORD'],
        "currentSchema": schema
    }

    df = spark_session.read.jdbc(
        url=f"jdbc:postgresql://{os.environ['POSTGRES_IP']}/{database}",
        properties=properties,
        table=table,
    )

    return df


def create_db_engine(database):
    db_connection = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
        f":5432/{database}",
    )

    return db_connection


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()

    return spark


class TimeSeries:
    """
    Contains common methods used when accessing time-series-based data,
    like heart rate, sleep, activity, etc. This class isn't intended to
    be used on it's own but serve as a base class for endpoint-specific
    classes.
    """
    def __init__(self, config):
        self.config = config
        self.authorized_client = get_authorized_client()
        return

    def query(self):
        """
        The two time-series based queries supported are documented here:
        https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
        :return:
        """
        try:
            assert len(self.config['base_date'].split('-')[0]) == 4
        except AssertionError:
            print('Dates must be formatted as YYYY-MM-DD. Exiting.')
            exit()

        base_date = datetime.strptime(self.config['base_date'], '%Y-%m-%d')
        period = self.config.get('period')

        if period:
            if period in ['1m', '30d']:
                self.config['end_date'] = base_date + timedelta(days=30)
            elif period in ['7d', '1w']:
                self.config['end_date'] = base_date + timedelta(days=7)
            elif period == '1d':
                self.config['end_date'] = base_date + timedelta(days=1)
            else:
                raise NotImplementedError(f'Period {period} is not supported.\n')

        data = self.authorized_client.time_series(
            resource=f'activities/{self.config["resource"]}',
            base_date=self.config['base_date'],
            end_date=self.config['end_date']
        )

        return data

    def insert_data(self):
        """
        Extracts, transforms & loads the data specified by the self.config dict.
        :return:
        """

        data = self.query()
        parsed_rows = self.parse_response(data)  # method should be implemented in inheriting class
        db = Database(self.config['database'], schema=self.config['schema'])

        # create a session connected to the database in config
        session = sessionmaker(bind=db.engine)()

        for row in tqdm(parsed_rows):
            try:
                session.add(row)
                session.commit()
            except FlushError:
                session.expunge_all()
                session.rollback()
                session.add(row)
                try:
                    session.commit()
                except IntegrityError:
                    session = handle_integrity_error(session=session, row=row)
                    continue
            except IntegrityError:
                session = handle_integrity_error(session, row)

        session.close()

        return parsed_rows

    def backfill(self, period: int = 90):
        """
        Backfills a database from the current day.
        Example: if run on 2020-09-06 with period=90, the database will populate for 2020-06-08 - 2020-09-06
        :param period: Number of days to look backward.
        :return:
        """
        self.config['base_date'] = (date.today() - timedelta(days=period)).strftime('%Y-%m-%d')
        self.config['end_date'] = date.today().strftime('%Y-%m-%d')

        self.insert_data()

    def plot(self):
        import matplotlib.pyplot as plt
        spark_session = SparkSession.builder.getOrCreate()

        properties = {
            "driver": "org.postgresql.Driver",
            "user": os.environ['POSTGRES_USERNAME'],
            "password": os.environ['POSTGRES_PASSWORD'],
            "currentSchema": 'heart'
        }

        df = spark_session.read.jdbc(
            url=f"jdbc:postgresql://{os.environ['POSTGRES_IP']}/fitbit",
            properties=properties,
            table=self.config['table'],
        )

        if self.config['resource'] == 'heart':
            agg_df = (
                df.groupBy(F.col('date')).agg(
                    F.sum(self.config.get('sum_column', 'calories')).alias(self.config.get('sum_column', 'calories'))
                ).orderBy('date')
            )

            agg_df = agg_df.toPandas()
            agg_df['calories'] = agg_df['calories'].astype(float)
            agg_df.plot(
                kind='bar',
                x='date',
                y='calories'
            )
            plt.show()
        else:
            print('Resource {} does not support plotting yet. Bug the developer!')

        return
