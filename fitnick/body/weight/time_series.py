from datetime import datetime

from fitnick.body.models.weight import WeightRecord
from fitnick.base.live_api import get_body_weight, get_health_provider, uses_live_health_api
from fitnick.time_series import TimeSeries


class WeightTimeSeries(TimeSeries):
    def __init__(self, config):
        super().__init__(config)
        self.config['schema'] = 'weight'
        self.config['resource'] = 'weight'
        return

    def query(self):
        if get_health_provider() == 'google' and uses_live_health_api():
            from fitnick.time_series import set_dates
            self.config = set_dates(self.config)
            return {
                'body-weight': [
                    {'dateTime': row['date'], 'value': row['kilograms'] * 2.2046226218}
                    for row in get_body_weight(
                        start_date=self.config['base_date'],
                        end_date=self.config['end_date'],
                    )
                ]
            }
        return super().query()

    @staticmethod
    def parse_response(data):
        rows = []
        for record in data['body-weight']:
            row = WeightRecord(
                date=record['dateTime'],
                pounds=record['value'])
            rows.append(row)

        return rows
