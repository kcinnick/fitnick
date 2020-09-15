from datetime import datetime

from sqlalchemy.exc import IntegrityError

from fitnick.base.base import get_authorized_client
from fitnick.database.database import Database
from fitnick.sleep.models import SleepSummary, sleep_summary_table


def parse_summary_response(sleep_data):
    date_of_sleep = sleep_data['dateOfSleep']
    summary_data = sleep_data['levels'].pop('summary')

    sleep_row = SleepSummary(
        # there's also count & thirtyDayAvgMinutes keys available in summary_data.
        date=datetime.strptime(date_of_sleep, '%Y-%m-%d'),
        deep=summary_data['deep']['minutes'],
        light=summary_data['light']['minutes'],
        rem=summary_data['rem']['minutes'],
        wake=summary_data['wake']['minutes'],
        total_minutes_asleep=sleep_data['minutesAsleep'],
        total_time_in_bed=sleep_data['timeInBed']
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

    def batch_query_sleep_data(self):
        """
        python-fitbit (the module) appears to only support the get_sleep method for one day,
        (see https://python-fitbit.readthedocs.io/en/latest/_modules/fitbit/api.html#Fitbit.get_sleep)
        but the API proper supports date range & list retrieval methods as well. This method specifically
        supports date range retrieval.
        :return:
        """
        response = self.authorized_client.make_request(
            method='get',
            url=f'https://api.fitbit.com/{self.authorized_client.API_VERSION}' +
                   f'/user/-/sleep/date/{self.config["base_date"]}/{self.config["end_date"]}.json',
            data={},
        )

        parsed_rows = []

        for row in response['sleep']:
            parsed_row = parse_summary_response(row)
            parsed_rows.append(parsed_row)

        return parsed_rows

    def insert_sleep_data(self, type_='summary'):
        if type_ == 'summary':
            sleep_data = self.query_sleep_data()
            rows = [parse_summary_response(sleep_data)]
        elif type_ == 'batch':
            rows = self.batch_query_sleep_data()
        else:
            print(f"type {type_} not yet supported.")
            return

        from sqlalchemy.dialects import postgresql
        db = Database(self.config['database'])
        connection = db.engine.connect()

        for row in rows:
            insert_stmt = postgresql.insert(sleep_summary_table, bind=db).values(
                date=row.date, deep=row.deep, light=row.light, rem=row.rem,
                wake=row.wake, total_minutes_asleep=row.total_minutes_asleep,
                total_time_in_bed=row.total_time_in_bed
            )
            try:
                connection.execute(insert_stmt)
            except IntegrityError:
                print('Data already exists for {}. Continuing.'.format(row.date))

        connection.close()

        return rows
