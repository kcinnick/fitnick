from datetime import datetime, timedelta, date
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import FlushError
from tqdm import tqdm

from fitnick.base.base import get_authorized_client, handle_integrity_error
from fitnick.database.database import Database


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

        if not self.config.get('end_date') and not period:
            self.config['end_date'] = self.config['base_date']
            #  if there's neither an end date or period specified,
            #  default to a 1d query.

        if self.config['resource'] in ['sleep', 'heart']:
            data = self.authorized_client.time_series(
                resource=f'activities/{self.config["resource"]}',
                base_date=self.config['base_date'],
                end_date=self.config['end_date']
            )
        elif self.config['resource'] in ['bmi', 'fat', 'weight']:
            data = self.authorized_client.time_series(
                resource=f'body/{self.config["resource"]}',
                base_date=self.config['base_date'],
                end_date=self.config['end_date']
            )
        else:
            raise NotImplementedError(f'Resource {self.config["resource"]} is not yet supported.\n')

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

    def insert_intraday_data(self):
        """
        Extracts, transforms & loads the data specified by the self.config dict.
        :return:
        """

        data = self.query()
        parsed_rows = self.parse_intraday_response(date=self.config['base_date'], intraday_response=data)
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
                session.expunge_all()
                session.rollback()
                continue

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
            "currentSchema": self.config['schema']
        }

        df = spark_session.read.jdbc(
            url=f"jdbc:postgresql://{os.environ['POSTGRES_IP']}/fitbit",
            properties=properties,
            table=self.config['table'],
        )

        if self.config['resource'] == 'heart':
            comparison = self.config.get('sum_column', 'calories')
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
        else:
            print('Resource {} does not support plotting yet. Bug the developer!')

        return
