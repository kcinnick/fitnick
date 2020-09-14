from datetime import date, datetime

from fitnick.base.base import get_authorized_client
from fitnick.database.database import Database
from fitnick.sleep.models import SleepSummary, sleep_summary_table


def parse_summary_response(sleep_data):
    date_of_sleep = sleep_data['sleep'][0]['dateOfSleep']
    summary_data = sleep_data.pop('summary')

    sleep_row = SleepSummary(
        date=datetime.strptime(date_of_sleep, '%Y-%m-%d'),
        deep=summary_data['stages']['deep'],
        light=summary_data['stages']['light'],
        rem=summary_data['stages']['rem'],
        wake=summary_data['stages']['wake'],
        total_minutes_asleep=summary_data['totalMinutesAsleep'],
        total_time_in_bed=summary_data['totalTimeInBed']
    )

    return sleep_row


class Sleep:
    def __init__(self, config):
        self.authorized_client = get_authorized_client()
        self.authorized_client.API_VERSION = '1.2'
        #  Fitbit is deprecating the 1 version of these endpoints, as described here:
        #  https://dev.fitbit.com/build/reference/web-api/sleep-v1/
        self.config = config

    def query_sleep_data(self):
        date = self.config['date']
        sleep_data = self.authorized_client.get_sleep(datetime.strptime(date, '%Y-%m-%d'))

        return sleep_data

    def insert_sleep_data(self, type_='summary'):
        sleep_data = self.query_sleep_data()
        if type_ == 'summary':
            row = parse_summary_response(sleep_data)
        else:
            print(f"type {type_} not yet supported.")
            return

        from sqlalchemy.dialects import postgresql
        db = Database(self.config['database'])
        connection = db.engine.connect()
        insert_stmt = postgresql.insert(sleep_summary_table, bind=db).values(
            date=row.date, deep=row.deep, light=row.light, rem=row.rem,
            wake=row.wake, total_minutes_asleep=row.total_minutes_asleep,
            total_time_in_bed=row.total_time_in_bed
        )

        connection.execute(insert_stmt)
        connection.close()

        return row
