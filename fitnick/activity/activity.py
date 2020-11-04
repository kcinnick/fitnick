import datetime
from datetime import timedelta

from pyspark.sql import functions as F
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from fitnick.activity.models.activity import ActivityLogRecord, activity_log_table
from fitnick.activity.models.calories import Calories, calories_table
from fitnick.base.base import get_authorized_client, get_df_from_db, create_spark_session
from fitnick.database.database import Database


class Activity:
    def __init__(self, config):
        self.config = config
        self.authorized_client = get_authorized_client()
        self.config['resource'] = 'activity'
        self.config['schema'] = 'activity'
        return

    def query_daily_activity_summary(self):
        """
        python-fitbit does not appear to support the web API's /activity/#get-daily-activity-summary
        endpoint, which returns the *actual* calories burned per day (i.e., the value shown on the FitBit app.)
        This method implements that endpoint, allowing for accurate calorie data collection.
        """
        response = self.authorized_client.make_request(
            method='get',
            url=f'https://api.fitbit.com/{self.authorized_client.API_VERSION}' +
                f'/user/-/activities/date/{self.config["base_date"]}.json',
            data={}
        )

        return response

    @staticmethod
    def parse_activity_log(response):
        """
        Given a JSON-formatted daily activity summary like the one returned by the query_daily_activity_summary,
        this method will create an ActivityLogRecord object for each item in the summary and return all of those
        objects in a list.
        :param response: JSON-formatted FitBit API response.
        :return:
        """
        rows = []

        for log in response['activities']:
            parsed_log = ActivityLogRecord(
                activity_id=log['activityId'], activity_name=log['activityParentName'], log_id=log['logId'],
                calories=log['calories'], distance=log['distance'], duration=log['duration'],
                duration_minutes=log['duration'] / 60000, start_date=log['startDate'], start_time=log['startTime'],
                steps=log['steps'])
            rows.append(parsed_log)

        return rows

    def query_calorie_summary(self):
        """
        Helper function for getting summary information only.
        :return: dict, daily activity summary JSON response
        """
        return self.query_daily_activity_summary()['summary']

    @staticmethod
    def parse_calorie_summary(date, response):
        """
        Given a JSON-formatted daily activity summary like the one returned by the query_calorie_summary method,
        this method creates & returns a Calories object.
        :param date: str, date of summary data
        :param response: dict, calorie summary data returned from FitBit API
        :return: Calories object, ready for database entry
        """
        row = Calories(
            date=date, total=response['caloriesOut'], calories_bmr=response['caloriesBMR'],
            activity_calories=response['activityCalories']
        )

        return row

    @staticmethod
    def insert_log_data(database, parsed_rows):
        """
        Inserts parsed ActivityLogRecord rows into the supplied database.
        :param database: fitnick.database Database object
        :param parsed_rows: list of ActivityLogRecord rows
        :return:
        """
        session = sessionmaker(bind=database.engine)()

        for row in parsed_rows:
            insert_statement = insert(activity_log_table).values(
                activity_id=row.activity_id,
                activity_name=row.activity_name,
                log_id=row.log_id,
                calories=row.calories,
                distance=row.distance,
                duration=row.duration,
                duration_minutes=row.duration_minutes,
                start_date=row.start_date,
                start_time=row.start_time,
                steps=row.steps)
            try:
                session.execute(insert_statement)
                session.commit()
            except IntegrityError:  # record already exists
                session.rollback()
                print(f'Log {row.log_id} already exists.')
                continue

        return parsed_rows

    @staticmethod
    def insert_calorie_data(database, parsed_row):
        """
        Inserts parsed Calories row into the supplied database.
        :param database: fitnick.database Database object
        :param parsed_row: list of Calories row
        :return:
        """
        session = sessionmaker(bind=database.engine)()

        insert_statement = insert(calories_table).values(
            date=parsed_row.date,
            total=parsed_row.total,
            calories_bmr=parsed_row.calories_bmr,
            activity_calories=parsed_row.activity_calories
        )

        update_statement = insert_statement.on_conflict_do_update(
            index_elements=['date'],
            set_={
                'date': parsed_row.date,
                'total': parsed_row.total,
                'calories_bmr': parsed_row.calories_bmr,
                'activity_calories': parsed_row.activity_calories
            })

        session.execute(update_statement)
        session.commit()

        return parsed_row

    def get_calories_for_day(self, day='2020-10-22'):
        """
        Given a YYYY-MM-DD formatted string, retrieves calorie data from the FitBit API
        and inserts it into the database specified by the config.
        :param day: str, day to get calorie data for
        :return:
        """
        self.config.update({'base_date': day})

        raw_calorie_summary = self.query_calorie_summary()
        row = self.parse_calorie_summary(self.config['base_date'], raw_calorie_summary)
        database = Database(self.config['database'], 'activity')
        self.insert_calorie_data(database, row)

        return row

    def get_lifetime_stats(self):
        """
        Implementation of the https://dev.fitbit.com/build/reference/web-api/activity/#get-lifetime-stats
        endpoint, which does not appear to be implemented in python-fitbit proper.
        :return:
        """
        response = self.authorized_client.make_request('https://api.fitbit.com/1/user/-/activities.json')

        best_stats = response.pop('best').get('total')

        lifetime_stats = response.pop('lifetime').get('total')
        lifetime_stats.pop('activeScore')
        lifetime_stats.pop('caloriesOut')  # removing both of these because they're always -1

        for key, value in best_stats.items():
            print('Best {}: {}'.format(key, value))

        print('\n')

        for key, value in lifetime_stats.items():
            print('Total {}: {}'.format(key, value))

        return lifetime_stats, best_stats

    def backfill_calories(self, period: int = 90):
        """
        Backfills a database from the current day.
        Example: if run on 2020-09-06 with period=90, the database will populate for 2020-06-08 - 2020-09-06
        :param period: Number of days to look backward.
        :return:
        """
        from datetime import date, timedelta
        import pandas as pd
        from tqdm import tqdm

        self.config['base_date'] = (date.today() - timedelta(days=period)).strftime('%Y-%m-%d')
        self.config['end_date'] = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # exclude current day

        date_range = pd.date_range(start=self.config['base_date'], end=self.config['end_date'], freq='D')
        date_range = [str(i).split()[0] for i in date_range]  # converts to str & removes unnecessary time string

        for day in tqdm(date_range):
            self.get_calories_for_day(day)

    def compare_calories_across_week(self, start_date, days_through_week):
        """
        This method compares calories burned between the start day's week & the week before.
        :param start_date: %Y-%m-%d string, date to base comparison on.
        :param days_through_week: int, days into current week to compare against

        There will always be 7 days of data for the last week, but when the current week is
        in progress, we need to tell the method how many days worth of last week's data to
        compare against so that it'll be a 1:1 comparison.

        mon: 1, tues: 2, weds: 3, thurs:4, fri: 5, sat: 6
        0 is not a valid input, because there are no completed days of
        calories to compare against before a day in that week ends. i.e.,
        there is no total Sunday data to compare against last Sunday until
        it's Monday.

        :return: tuple containing the summed calories for the week of the start_date & the week preceding.
        """
        spark_session = create_spark_session()
        df = get_df_from_db(
            database=self.config['database'], schema='activity', table='calories',
            spark_session=spark_session
        )

        df = df.withColumn('day_of_year', F.dayofyear(df.date))
        start_date_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        first_week_df = df.where(df.date.between(
            start_date,
            start_date_datetime + timedelta(days_through_week - 1))
        )

        next_week_df = df.where(df.date.between(
            start_date_datetime + timedelta(days=7),
            start_date_datetime + timedelta(days=7) + timedelta(days=days_through_week - 1))
        )

        first_week_total = first_week_df.select(F.sum(first_week_df.total)).collect()[0][0]
        next_week_total = next_week_df.select(F.sum(next_week_df.total)).collect()[0][0]

        print(
            "You had burned {} calories at this point during the week of {}, compared to {} the following week.".format(
                first_week_total,
                start_date,
                next_week_total)
        )

        return next_week_total, first_week_total
