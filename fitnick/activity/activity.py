from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from fitnick.base.base import get_authorized_client
from fitnick.activity.models.activity import ActivityLogRecord


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
    def insert_data(database, parsed_rows):
        session = sessionmaker(bind=database.engine)()
        for row in tqdm(parsed_rows):
            session.expunge_all()
            session.add(row)
            session.commit()

        session.close()

        return parsed_rows
