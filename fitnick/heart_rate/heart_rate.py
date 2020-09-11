from datetime import date, datetime, timedelta
from fitnick.heart_rate.models import heart_daily_table

from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from fitnick.base.base import create_db_engine, get_authorized_client
from fitnick.heart_rate.models import HeartDaily


class HeartRateZone:
    def __init__(self, config):
        self.authorized_client = get_authorized_client()
        self.config = config

    def query_heart_rate_zone_time_series(self):
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
            resource='activities/heart',
            base_date=self.config['base_date'],
            end_date=self.config['end_date']
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

    def insert_heart_rate_time_series_data(self, connection):
        """
        Extracts, transforms & loads the data specified by the self.config dict.
        :param connection: SQLAlchemy database connection.
        :return:
        """
        data = self.query_heart_rate_zone_time_series()
        parsed_rows = self.parse_response(data)
        for row in tqdm(parsed_rows):
            insert_cmd = heart_daily_table.insert().values(
                type=row.type, minutes=row.minutes, date=row.date,
                calories=row.calories, resting_heart_rate=row.resting_heart_rate)
            try:
                connection.execute(insert_cmd)
            except IntegrityError:
                continue

        return parsed_rows

    def get_heart_rate_zone_for_day(self, database: str, target_date: str = 'today'):
        """
        Retrieves heart rate data for one day only.
        This method should not be used to add batch data - i.e., iterating
        through a list of dates is likely to trigger rate limit errors.
        :param database: Database to insert into.
        :param target_date: Date to retrieve heart rate zone data for.
        :return:
        """
        # add a check to only get this if we don't already have it
        if target_date != 'today':
            self.config.update({
                'base_date': target_date,
                'database': database,
                'period': '1d'}
            )
        else:
            self.config.update({
                'base_date': date.today().strftime('%Y-%m-%d'),
                'database': database,
                'period': '1d'}
            )

        db = create_db_engine(self.config['database'])
        rows = self.insert_heart_rate_time_series_data(connection=db.engine.connect())

        return rows

    def backfill(self, database: str, period: int = 90):
        """
        Backfills a database from the current day.
        Example: if run on 2020-09-06 with period=90, the database will populate for 2020-06-08 - 2020-09-06
        :param database: Name of database to insert into. Options are fitbit & fitbit_test
        :param period: Number of days to look backward.
        :return:
        """
        self.config['base_date'] = (date.today() - timedelta(days=period)).strftime('%Y-%m-%d')
        self.config['end_date'] = date.today().strftime('%Y-%m-%d')
        db = create_db_engine(database)

        self.insert_heart_rate_time_series_data(db.connect())
