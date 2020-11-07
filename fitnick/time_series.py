from datetime import datetime, timedelta, date
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from fitnick.base.base import get_authorized_client
from fitnick.database.database import Database


def plot(config):
    import matplotlib.pyplot as plt
    spark_session = SparkSession.builder.getOrCreate()

    properties = {
        "driver": "org.postgresql.Driver",
        "user": os.environ['POSTGRES_USERNAME'],
        "password": os.environ['POSTGRES_PASSWORD'],
        "currentSchema": config['schema']
    }

    df = spark_session.read.jdbc(
        url=f"jdbc:postgresql://{os.environ['POSTGRES_IP']}/{config['database']}",
        properties=properties,
        table=config['table'],
    )

    if config['resource'] == 'heart':
        comparison = config.get('sum_column', 'calories')
        agg_df = (
            df.groupBy(F.col('date')).agg(
                F.sum(comparison).alias(comparison)
            ).orderBy('date')
        )

        agg_df = agg_df.toPandas()
        agg_df[comparison] = agg_df[comparison].astype(float)
        agg_df.plot(
            kind='bar',
            x='date',
            y=comparison
        )
        plt.show()
    elif config['resource'] == 'weight':
        """parsing for weight"""
        df = df.orderBy('date').toPandas()
        df['pounds'] = df['pounds'].astype(float)
        df.plot(
            x='date',
            y='pounds'
        )
        plt.show()
    else:
        print('Resource {} does not support plotting yet. Bug the developer!'.format(config['resource']))

    return


def set_dates(config):
    try:
        assert len(config['base_date'].split('-')[0]) == 4
    except AssertionError:
        print('Dates must be formatted as YYYY-MM-DD. Exiting.')
        exit()

    base_date = datetime.strptime(config['base_date'], '%Y-%m-%d')
    period = config.get('period')

    if period:
        if period in ['1m', '30d']:
            config['end_date'] = (base_date + timedelta(days=30)).date()
        elif period in ['7d', '1w']:
            config['end_date'] = (base_date + timedelta(days=7)).date()
        elif period == '1d':
            config['end_date'] = (base_date + timedelta(days=1)).date()
        else:
            raise NotImplementedError(f'Period {period} is not supported.\n')

    if not config.get('end_date') and not period:
        config['end_date'] = config['base_date']
        #  if there's neither an end date or period specified,
        #  default to a 1d query.

    return config


def plot_rolling_average(config, days=3):
    import matplotlib.pyplot as plt
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    from fitnick.base.base import get_df_from_db, create_spark_session

    spark_session = create_spark_session()

    schema = config['schema']
    table = config['table']
    sum_column = config['sum_column']

    df = get_df_from_db(spark_session, database='fitbit', schema=schema, table=table)

    agg_df = df.groupBy(F.col('date')).agg(F.sum(sum_column)).alias(sum_column)
    agg_df = agg_df.filter(F.col('date').between(config['base_date'], config['end_date']))

    window_spec = Window.orderBy(F.col("date")).rowsBetween(-days, 0)

    agg_df = agg_df.withColumn(f'{days}DMA', F.avg(f"{sum_column}.sum({sum_column})").over(window_spec))
    agg_df = agg_df.toPandas()

    agg_df[f'{days}DMA'] = agg_df[f'{days}DMA'].astype(float)
    agg_df[f'sum({sum_column})'] = agg_df[f'sum({sum_column})'].astype(float)
    agg_df.plot(
        kind='line',
        x='date',
        y=[f'{days}DMA', f'sum({sum_column})'],
    )

    plt.show()

    return


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
        self.config = set_dates(self.config)

        if self.config['resource'] in ['sleep', 'heart', 'steps', 'calories', 'caloriesBMR', 'distance',
                                       'floors', 'elevation', 'minutesSedentary', 'minutesLightlyActive',
                                       'minutesFairlyActive', 'minutesVeryActive', 'activityCalories']:
            data = self.authorized_client.time_series(
                resource=f'activities/{self.config["resource"]}',
                base_date=self.config['base_date'],
                end_date=self.config['end_date']
            )
        elif self.config['resource'] in ['bmi', 'weight']:
            data = self.authorized_client.time_series(
                resource=f'body/{self.config["resource"]}',
                base_date=self.config['base_date'],
                end_date=self.config['end_date']
            )
        else:
            raise NotImplementedError(f'Resource {self.config["resource"]} is not yet supported.\n')

        return data

    @staticmethod
    def parse_response(data):
        """
        Method needs to be overwritten in inheriting class.
        :param data:
        :return:
        """
        return data

    def insert_data(self, database, table):
        """
        Extracts, transforms & loads the data specified by the self.config dict.
        :return:
        """
        self.validate_input()
        data = self.query()
        parsed_rows = self.parse_response(data)  # method should be implemented in inheriting class
        # create a session connected to the database in config
        session = sessionmaker(bind=database.engine)()

        if table.fullname == 'heart.daily':
            for row in parsed_rows:
                insert_statement = insert(table).values(
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
        elif table.fullname == 'weight.daily':
            for row in parsed_rows:
                insert_statement = insert(table).values(
                    date=row.date,
                    pounds=row.pounds
                )
                session.execute(insert_statement)
                session.commit()

        return parsed_rows

    def insert_intraday_data(self):
        """
        Extracts, transforms & loads the intraday data specified by the self.config dict.
        :return:
        """

        data = self.query()
        parsed_rows = self.parse_intraday_response(date=self.config['base_date'], intraday_response=data)
        db = Database(self.config['database'], schema=self.config['schema'])

        # create a session connected to the database in config
        session = sessionmaker(bind=db.engine)()

        for row in tqdm(parsed_rows):
            session.add(row)
            session.commit()

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

        database = Database(database=self.config['database'], schema=self.config['schema'])
        self.insert_data(database=database, table=self.config['table'])

    def validate_input(self):
        try:
            assert re.match(r'\d{4}-\d{2}-\d{2}', self.config['base_date']).group()
        except AttributeError as e:
            print('Start date must be formatted as YYYY/MM/DD.')
            raise e

        if 'end_date' in self.config.keys():
            try:
                assert re.match(r'\d{4}-\d{2}-\d{2}', self.config['end_date']).group()
            except AttributeError as e:
                print('End date must be formatted as YYYY/MM/DD.')
                raise e
        elif 'period' in self.config.keys():
            pass

        return True
