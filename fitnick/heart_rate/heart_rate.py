from datetime import date, timedelta

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm.exc import FlushError
from tqdm import tqdm

from fitnick.base.base import create_db_engine, get_authorized_client
from fitnick.heart_rate.models import HeartDaily


def rollback_and_commit(session, row):
    session.rollback()
    session.add(row)
    session.commit()


def update_old_rows(session, row):
    session.rollback()
    session.commit()
    rows = session.query(HeartDaily).filter_by(date=row.date).filter_by(type=row.type).all()
    for old_row in rows:
        if old_row.minutes != row.minutes:  # if there's a discrepancy, delete the old row & add the new one
            session.delete(old_row)
            session.commit()
            session.add(row)
            session.commit()
        else:
            continue
    return


class HeartRateZone:
    def __init__(self, config):
        self.authorized_client = get_authorized_client()
        self.config = config

    def query_heart_rate_zone_time_series(self):
        """
        The two time-series based queries supported are documented here:
        https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
        :param config: dict containing the settings that determine what kind of time-series request gets made.
        :return:
        """
        try:
            assert len(self.config['base_date'].split('-')[0]) == 4
        except AssertionError:
            print('Dates must be formatted as YYYY-MM-DD. Exiting.')
            exit()

        if self.config.get('end_date'):
            data = self.authorized_client.time_series(
                resource='activities/heart',
                base_date=self.config['base_date'],
                end_date=self.config['end_date']
            )
        else:
            # we're assuming that if it's not a daterange search, it's a period search.
            # period searches look backwards for a base_date - i.e., a base_date of
            # 2020-09-02 will cover 2020-08-27 to 2020-09-02
            data = self.authorized_client.time_series(
                resource='activities/heart',
                base_date=self.config['base_date'],
                period=self.config['period']
            )

        return data

    @staticmethod
    def parse_response(data):
        rows = []
        for day in data['activities-heart']:
            date = day['dateTime']
            try:
                resting_heart_rate = day['value']['restingHeartRate']
            except KeyError:
                resting_heart_rate = 0
            for heart_rate_zone in day['value']['heartRateZones']:
                row = HeartDaily(
                    type=heart_rate_zone['name'],
                    minutes=heart_rate_zone.get('minutes', 0),
                    date=date,
                    calories=heart_rate_zone.get('caloriesOut', 0),
                    resting_heart_rate=resting_heart_rate
                )
                rows.append(row)

        return rows

    def insert_heart_rate_time_series_data(self, raise_exception=False):
        data = self.query_heart_rate_zone_time_series()
        parsed_rows = self.parse_response(data)
        db = create_db_engine(self.config['database'])
        session = sessionmaker(bind=db)
        session = session()
        for row in tqdm(parsed_rows):
            session.add(row)

        try:
            session.commit()
        except IntegrityError as e:
            if raise_exception:
                raise e
            else:
                pass

        return parsed_rows

    def get_today_heart_rate_time_series_data(self, database):
        self.config.update({
            'base_date': date.today().strftime('%Y-%m-%d'),
            'database': database,
            'period': '1d'}
        )
        rows = self.insert_heart_rate_time_series_data()

        return rows

    def get_heart_rate_zone_for_day(self, database, target_date):
        # add a check to only get this if we don't already have it
        self.config.update({
            'base_date': target_date,
            'database': database,
            'period': '1d'}
        )

        rows = self.insert_heart_rate_time_series_data()

        return rows

    def backfill(self, database: str, period: int = 90):
        """
        Backfills a database from the current day.
        Example: if run on 2020-09-06 with period=90, the database will populate for 2020-06-08 - 2020-09-06
        :param database:
        :param period:
        :return:
        """
        backfill_date = date.today() - timedelta(days=period)
        for day in range(period):
            backfill_day = (backfill_date + timedelta(days=day)).strftime('%Y-%m-%d')
            print(f'Retrieving heart rate zone data for {backfill_day}.\n')
            self.get_heart_rate_zone_for_day(database, target_date=backfill_day)

        return
