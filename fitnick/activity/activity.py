from fitnick.base.base import get_authorized_client
from fitnick.activity.models.activity import ActivityLogEntry


class Activity:
    def __init__(self, config):
        self.config = config
        self.authorized_client = get_authorized_client()
        self.config['resource'] = 'activity'
        self.config['schema'] = 'activity'
        return

    def query_daily_activity_summary(self):
        """
        python-fitbit does not appear to support the web API's /activity/#get-daily-activity-summary
        endpoint, which returns the *actual* calories burned per day (i.e., the value shown on the FitBit app.)
        This method implements that endpoint, allowing for accurate calorie data collection.
        """
        response = self.authorized_client.make_request(
            method='get',
            url=f'https://api.fitbit.com/{self.authorized_client.API_VERSION}' +
                   f'/user/-/activities/date/{self.config["base_date"]}.json',
            data={}
        )

        return response
