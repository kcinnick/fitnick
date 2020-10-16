from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from fitnick.body.models.weight import WeightRecord, weight_table
from fitnick.database.database import Database
from fitnick.time_series import TimeSeries


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
