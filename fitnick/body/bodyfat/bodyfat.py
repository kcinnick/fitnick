from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from fitnick.base.base import get_authorized_client
from fitnick.body.models.bodyfat import BodyFatRecord, bodyfat_table
from fitnick.database.database import Database
from fitnick.time_series import set_dates


class BodyFat:
    def __init__(self, config):
        self.config = {'schema': 'bodyfat', 'resource': 'fat'}
        self.config.update(config)
        self.authorized_client = get_authorized_client()

    def query(self):
        # set base & end date if this is a period search
        config = set_dates(self.config)
        self.config.update(config)

        #  unlike the 'steps' or 'heart-rate' kind of activities, there are only
        #  entries for body fat if the authorized user proactively creates them.
        #  because of this, some unexpected behavior may occur - i.e. querying 1m
        #  will only return as many entries as there were in that month, not 30/31.

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

    def insert_data(self):
        """
        Extracts, transforms & loads the data specified by the self.config dict.
        :return:
        """

        data = self.query()
        parsed_rows = self.parse_response(data)
        db = Database(self.config['database'], schema=self.config['schema'])
        session = sessionmaker(bind=db.engine)()
        for row in parsed_rows:
            insert_statement = insert(bodyfat_table).values(
                date=row.date,
                fat=row.fat,
                logid=row.logId,
                source=row.source,
                time=row.time
            )
            try:
                session.execute(insert_statement)
                session.commit()
            except IntegrityError:  # record already exists
                pass

        session.close()

    def get_bodyfat_rows(self):
        response = self.query()
        parsed_rows = self.parse_response(response)

        return parsed_rows

    def log(self, date, fat):
        """
        Logs a new body fat entry to the Fitbit device (not the database).
        :param date: date to log for
        :param fat: decimal fat value to log
        :return:
        """
        response = self.authorized_client.make_request(
            method='post',
            url=f'https://api.fitbit.com/{self.authorized_client.API_VERSION}' +
                   f'/user/-/body/log/fat.json',
            data={'date': date,
                  'fat': fat}
        )

        return response['fatLog']

    def delete(self, log_id):
        """
        As the documentation states, "A successful request returns a 204 status code with an empty response body."
        :param log_id: ID of body fat log to delete
        :return:
        """
        from fitbit.exceptions import BadResponse, HTTPNotFound

        try:
            self.authorized_client.make_request(
                method='delete',
                url=f'https://api.fitbit.com/{self.authorized_client.API_VERSION}' +
                       f'/user/-/body/log/fat/{log_id}.json'
            )
        except BadResponse:
            print('Log deleted.')
        except HTTPNotFound:
            print(f'Log with ID {log_id} not found.')

        return True
