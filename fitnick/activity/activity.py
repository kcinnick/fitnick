from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from fitnick.base.base import get_authorized_client
from fitnick.activity.models.activity import ActivityLogRecord, activity_log_table
from fitnick.activity.models.calories import Calories, calories_table


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
        rows = []

        for log in response['activities']:
            parsed_log = ActivityLogRecord(
                activity_id=log['activityId'], activity_name=log['activityParentName'], log_id=log['logId'],
                calories=log['calories'], distance=log['distance'], duration=log['duration'],
                duration_minutes=log['duration'] / 60000, start_date=log['startDate'], start_time=log['startTime'],
                steps=log['steps'])
            rows.append(parsed_log)

        return rows

    @staticmethod
    def insert_log_data(database, parsed_rows):
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

    def query_calorie_summary(self):
        return self.query_daily_activity_summary()['summary']

    @staticmethod
    def parse_calorie_summary(date, response):
        row = Calories(
            date=date, total=response['caloriesOut'], calories_bmr=response['caloriesBMR'],
            activity_calories=response['activityCalories']
        )

        return row

    @staticmethod
    def insert_calorie_data(database, parsed_row):
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
