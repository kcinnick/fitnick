from fitnick.body.models.weight import WeightRecord
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
