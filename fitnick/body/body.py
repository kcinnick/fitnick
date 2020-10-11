from fitnick.base.base import get_authorized_client
from sqlalchemy.orm import sessionmaker

from fitnick.database.database import Database
from fitnick.time_series import TimeSeries, set_dates
from fitnick.body.models.bodyfat import BodyFatRecord, bodyfat_table
from fitnick.body.models.weight import WeightRecord, weight_table

from sqlalchemy.dialects.postgresql import insert


class WeightTimeSeries(TimeSeries):
    def __init__(self, config):
        super().__init__(config)
        self.config['schema'] = 'weight'
        self.config['resource'] = 'weight'
        return

    @staticmethod
    def parse_response(data):
        rows = []
        for record in data['body-weight']:
            row = WeightRecord(
                date=record['dateTime'],
                pounds=record['value'])
            rows.append(row)

        return rows

    def insert_data(self):
        data = self.query()
        parsed_rows = self.parse_response(data)
        db = Database(self.config['database'], schema=self.config['schema'])
        session = sessionmaker(bind=db.engine)()
        for row in parsed_rows:
            insert_statement = insert(weight_table).values(
                date=row.date,
                pounds=row.pounds
            )
            session.execute(insert_statement)
            session.commit()

        session.close()
        return


class BodyFat:
    def __init__(self, config):
        self.config = {'schema': 'weight', 'resource': 'fat'}
        self.config.update(config)
        self.authorized_client = get_authorized_client()

    def query(self):
        # set base & end date if this is a period search
        config = set_dates(self.config)
        self.config.update(config)

        #  unlike the 'steps' or 'heart-rate' kind of activities, there are only
        #  entries for body fat if the authorized user proactively creates them.
        #  because of this, some unexpected behavior may occur - i.e. querying 1m
        #  will only return as many entries as there were in that month, not 31.

        response = self.authorized_client.make_request(
            method='get',
            url=f'https://api.fitbit.com/{self.authorized_client.API_VERSION}' +
                   f'/user/-/body/log/fat/date/{self.config["base_date"]}/{self.config["end_date"]}.json')

        return response

    @staticmethod
    def parse_response(response):
        rows = []
        for record in response['fat']:
            row = BodyFatRecord(
                date=record['date'],
                fat=record['fat'],
                logId=record['logId'],
                source=record['source'],
                time=record['time']
            )
            rows.append(row)

        return rows
