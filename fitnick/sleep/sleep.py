from datetime import datetime

from sqlalchemy.exc import IntegrityError

from fitnick.base.base import get_authorized_client, TimeSeries
from fitnick.database.database import Database
from fitnick.sleep.models import SleepSummary, sleep_summary_table


class SleepTimeSeries(TimeSeries):
    def __init__(self, config):
        super().__init__(config)
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
            parsed_row = self.parse_response(row)
            parsed_rows.append(parsed_row)

        return parsed_rows

    @staticmethod
    def parse_response(sleep_data):
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

        return [sleep_row]
