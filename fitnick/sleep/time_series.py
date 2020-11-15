from pprint import pprint

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from fitnick.base.base import get_authorized_client
from fitnick.time_series import TimeSeries
from fitnick.sleep.models import SleepSummary, SleepLevel, sleep_summary_table, sleep_level_table


class SleepTimeSeries(TimeSeries):
    def __init__(self, config):
        super().__init__(config.update({'resource': 'sleep'}))
        self.authorized_client = get_authorized_client()
        self.authorized_client.API_VERSION = '1.2'
        #  Fitbit is deprecating the 1 version of these endpoints, as described here:
        #  https://dev.fitbit.com/build/reference/web-api/sleep-v1/
        self.config = config

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

        return response

    @staticmethod
    def parse_response(data):
        levels = data.pop('levels')

        sleep_summary_row = SleepSummary(
            date=data['dateOfSleep'],
            duration=data['duration'],
            efficiency=data['efficiency'],
            end_time=data['endTime'],
            minutes_asleep=data['minutesAsleep'],
            minutes_awake=data['minutesAwake'],
            start_time=data['startTime'],
            time_in_bed=data['timeInBed'],
            log_id=data['logId']
        )

        sleep_levels = levels['summary']

        parsed_sleep_level_rows = [SleepLevel(
            date=data['dateOfSleep'], type_=key, count_=value['count'],
            minutes=value['minutes'], thirty_day_avg_minutes=value.get('thirtyDayAvgMinutes'))
            for key, value in sleep_levels.items()]

        parsed_sleep_level_rows.append(sleep_summary_row)

        return parsed_sleep_level_rows

    def insert_data(self, database, **kwargs):
        """
        Extracts, transforms & loads sleep summary & sleep level data for the date(s) specified
        in the config.
        :param database: Database object connected to fitbit or fitbit_test.
        :return:
        """
        response = self.batch_query_sleep_data()
        for day in response['sleep']:
            parsed_rows = self.parse_response(day)
            session = sessionmaker(bind=database.engine)()

            level_rows = parsed_rows[:-1]
            summary_row = parsed_rows[-1]

            summary_insert_statement = insert(sleep_summary_table).values(
                date=summary_row.date,
                duration=summary_row.duration,
                efficiency=summary_row.efficiency,
                end_time=summary_row.end_time,
                minutes_asleep=summary_row.minutes_asleep,
                minutes_awake=summary_row.minutes_awake,
                start_time=summary_row.start_time,
                time_in_bed=summary_row.time_in_bed,
                log_id=summary_row.log_id
            )

            try:
                session.execute(summary_insert_statement)
                session.commit()
            except IntegrityError:  # record already exists
                session.rollback()
                pass

            level_insert_statements = [insert(sleep_level_table).values(
                date=i.date, type_=i.type_, count_=i.count_, minutes=i.minutes,
                thirty_day_avg_minutes=i.thirty_day_avg_minutes) for i in level_rows
            ]

            for level_insert_statement in level_insert_statements:
                try:
                    session.execute(level_insert_statement)
                    session.commit()
                except IntegrityError:  # record already exists
                    session.rollback()
                    pass

        return
