from fitnick.base.base import get_authorized_client
from sqlalchemy.orm import sessionmaker

from fitnick.database.database import Database
from fitnick.time_series import TimeSeries, set_dates
from fitnick.body.models import WeightRecord, weight_table

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

        data = self.authorized_client.make_request(
            method='get',
            url=f'https://api.fitbit.com/{self.authorized_client.API_VERSION}' +
                   f'/user/-/body/log/fat/date/{self.config["base_date"]}/{self.config["end_date"]}.json')

        return data
