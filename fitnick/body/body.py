from fitnick.time_series import TimeSeries
from fitnick.body.models import WeightRecord, weight_table


class WeightTimeSeries(TimeSeries):
    def __init__(self, config):
        super().__init__(config)
        self.config['schema'] = 'body'
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
