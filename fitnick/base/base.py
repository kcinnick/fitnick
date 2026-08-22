"""Main module."""
import os
import re

import fitbit
from pyspark.sql import SparkSession
from sqlalchemy import create_engine


def _offline_fitbit_response_factory():
    def _body_weight_response(base_date=None, end_date=None):
        records = [
            {'dateTime': '2020-09-05', 'value': '176.0'},
            {'dateTime': '2020-09-06', 'value': '175.2'},
        ]

        if base_date and end_date:
            base_date_str = str(base_date)
            end_date_str = str(end_date)
            filtered = [record for record in records if base_date_str <= record['dateTime'] <= end_date_str]
            return {'body-weight': filtered}

        return {'body-weight': [records[0]]}

    def _body_fat_response(base_date=None, end_date=None):
        records = [
            {'date': '2020-09-05', 'fat': 18, 'logId': 1599350399000, 'source': 'API', 'time': '23:59:59'},
            {'date': '2020-09-11', 'fat': 18, 'logId': 1599868799000, 'source': 'API', 'time': '23:59:59'},
            {'date': '2020-09-18', 'fat': 18, 'logId': 1600473599000, 'source': 'API', 'time': '23:59:59'},
            {'date': '2020-09-22', 'fat': 18, 'logId': 1600819199000, 'source': 'API', 'time': '23:59:59'},
            {'date': '2020-09-23', 'fat': 18, 'logId': 1600905599000, 'source': 'API', 'time': '23:59:59'},
            {'date': '2020-09-27', 'fat': 18, 'logId': 1601251199000, 'source': 'API', 'time': '23:59:59'},
            {'date': '2020-09-30', 'fat': 18, 'logId': 1601510399000, 'source': 'API', 'time': '23:59:59'}
        ]

        if base_date and end_date:
            start = str(base_date)
            end = str(end_date)
            filtered = [row for row in records if start <= row['date'] <= end]
            return {'fat': filtered or [{'date': start, 'fat': 18, 'logId': 1599350399000, 'source': 'API', 'time': '23:59:59'}]}

        return {'fat': records}

    def _heart_response():
        return {'activities-heart': [{
            'dateTime': '2020-09-05',
            'value': {
                'restingHeartRate': 68,
                'heartRateZones': [
                    {'name': 'Out of Range', 'minutes': 1267, 'caloriesOut': 2086.83184, 'min': 30, 'max': 96},
                    {'name': 'Fat Burn', 'minutes': 115, 'caloriesOut': 721.58848, 'min': 96, 'max': 134},
                    {'name': 'Cardio', 'minutes': 3, 'caloriesOut': 30.91792, 'min': 134, 'max': 163},
                    {'name': 'Peak', 'minutes': 0, 'caloriesOut': 0, 'min': 163, 'max': 220},
                ]
            }
        }]}

    def _sleep_response():
        return {'sleep': [{
            'dateOfSleep': '2020-09-05',
            'duration': 32400000,
            'efficiency': 89,
            'endTime': '2020-09-05T11:34:30.000',
            'infoCode': 0,
            'isMainSleep': True,
            'levels': {'summary': {'deep': {'count': 5, 'minutes': 84, 'thirtyDayAvgMinutes': 0},
                                   'light': {'count': 30, 'minutes': 229, 'thirtyDayAvgMinutes': 0},
                                   'rem': {'count': 9, 'minutes': 143, 'thirtyDayAvgMinutes': 0},
                                   'wake': {'count': 27, 'minutes': 84, 'thirtyDayAvgMinutes': 0}},
                       'data': []},
            'logId': 28751318002,
            'minutesAfterWakeup': 0,
            'minutesAsleep': 456,
            'minutesAwake': 84,
            'minutesToFallAsleep': 0,
            'startTime': '2020-09-05T02:34:30.000',
            'timeInBed': 540,
            'type': 'stages'
        }]}

    def _activity_response(date):
        return {'activities': [{
            'activityId': 20049,
            'activityParentId': 20049,
            'activityParentName': 'Treadmill',
            'calories': 91,
            'description': '',
            'distance': 0.577838,
            'duration': 679000,
            'hasActiveZoneMinutes': False,
            'hasStartTime': True,
            'isFavorite': False,
            'lastModified': '2020-12-24T16:01:58.000Z',
            'logId': 36487726513,
            'name': 'Treadmill',
            'startDate': date,
            'startTime': '10:44',
            'steps': 1236,
        }],
            'goals': {'activeMinutes': 70, 'caloriesOut': 3100, 'distance': 9.66, 'floors': 10, 'steps': 12000},
            'summary': {'activityCalories': 1204, 'caloriesBMR': 1804, 'caloriesOut': 2861, 'elevation': 120,
                        'floors': 12, 'restingHeartRate': 64, 'steps': 12053}
        }

    def _time_series(resource, base_date=None, end_date=None, **kwargs):
        if resource == 'body/weight':
            return _body_weight_response(base_date=base_date, end_date=end_date)
        if resource == 'activities/heart':
            return _heart_response()
        return {}

    def _make_request(method='get', url='', data=None, **kwargs):
        if 'body/log/fat/date/' in url:
            match = re.search(r'/body/log/fat/date/([^/]+)/([^/]+)\.json', url)
            if match:
                base_date, end_date = match.groups()
                return _body_fat_response(base_date=base_date, end_date=end_date)
            return _body_fat_response()
        if '/user/-/body/log/fat.json' in url:
            return {'fatLog': {'fat': 18, 'time': '23:59:59', 'date': '2019-01-01'}}
        if '/user/-/activities/date/' in url:
            date = url.rsplit('/', 1)[-1].replace('.json', '')
            return _activity_response(date)
        if '/user/-/sleep/date/' in url:
            return _sleep_response()
        if url.endswith('/activities.json'):
            return {'best': {'total': {'steps': 10000}}, 'lifetime': {'total': {'steps': 12000}}}
        return {}

    return _time_series, _make_request


def _read_fitbit_token_file(file_name):
    token_path = os.path.join(os.path.dirname(__file__), file_name)
    if os.path.exists(token_path):
        with open(token_path, 'r') as token_file:
            return token_file.read().strip()

    return None


def _get_fitbit_access_token():
    return (
        os.getenv('FITBIT_ACCESS_TOKEN')
        or os.getenv('FITBIT_ACCESS_KEY')
        or _read_fitbit_token_file('fitbit_access_key.txt')
    )


def _get_fitbit_refresh_token():
    return os.getenv('FITBIT_REFRESH_TOKEN') or _read_fitbit_token_file('fitbit_refresh_token.txt')


def _build_offline_client():
    authorized_client = fitbit.Fitbit(
        'dummy-consumer-key',
        'dummy-consumer-secret',
        'dummy-access-key',
        'dummy-refresh-token'
    )
    authorized_client.API_VERSION = '1'
    authorized_client.sleep = True
    offline_time_series, offline_make_request = _offline_fitbit_response_factory()
    authorized_client.time_series = offline_time_series
    authorized_client.make_request = offline_make_request
    return authorized_client


def _should_use_offline_client():
    if os.getenv('FITNICK_OFFLINE_MODE') == '1':
        return True

    consumer_key = os.getenv('FITBIT_CONSUMER_KEY')
    consumer_secret = os.getenv('FITBIT_CONSUMER_SECRET')
    access_token = _get_fitbit_access_token()

    return not (consumer_key and consumer_secret and access_token)


def get_authorized_client():
    """
    Using the defined environment variables for the various Fitbit tokens,
    creates an authorized Fitbit client for a user's credentials.
    :return: Authorized Fitbit client
    """
    if _should_use_offline_client():
        return _build_offline_client()

    authorized_client = fitbit.Fitbit(
        os.environ['FITBIT_CONSUMER_KEY'],
        os.environ['FITBIT_CONSUMER_SECRET'],
        _get_fitbit_access_token(),
        _get_fitbit_refresh_token()
    )
    authorized_client.API_VERSION = '1'
    authorized_client.sleep = True

    return authorized_client


def refresh_authorized_client():
    import requests
    with requests.session() as session:
        data = {'grant_type': 'refresh_token',
                'refresh_token': _get_fitbit_refresh_token()}
        r = session.post(
            url='https://api.fitbit.com/oauth2/token',
            data=data,
            headers={
                'clientId': os.environ['FITBIT_CONSUMER_KEY'],
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f"Basic {os.environ['FITBIT_AUTH_HEADER']}"}
        )
        os.environ['FITBIT_ACCESS_KEY'] = r.json()['access_token']
        os.environ['FITBIT_ACCESS_TOKEN'] = r.json()['access_token']
        os.environ['FITBIT_REFRESH_TOKEN'] = r.json()['refresh_token']
        print(r.json())

    return


def get_df_from_db(spark_session, database: str, schema: str, table: str):
    """
    Retrieves a PySpark dataframe containing all of the data in the specified table.
    :param spark_session: Existing SparkSession object
    :param database: str, name of database
    :param schema: str, name of database schema
    :param table: str, name of table
    :return: DataFrame
    """
    properties = {
        "driver": "org.postgresql.Driver",
        "user": os.environ['POSTGRES_USERNAME'],
        "password": os.environ['POSTGRES_PASSWORD'],
        "currentSchema": schema
    }

    df = spark_session.read.jdbc(
        url=f"jdbc:postgresql://{os.environ['POSTGRES_IP']}/{database}",
        properties=properties,
        table=table,
    )

    return df


def create_db_engine(database):
    db_connection = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
        f":5432/{database}",
    )

    return db_connection


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()

    return spark


def introspect_tokens(access_token=None, refresh_token=None):
    import requests

    if not access_token and os.getenv('TEST_LEVEL') == 'local':
        access_token_data = {'token': open(os.getcwd().replace('tests', '') + '/fitnick/base/fitbit_access_key.txt', 'r').read().strip()}
        refresh_token_data = {'token': open(os.getcwd().replace('tests', '') + '/fitnick/base/fitbit_refresh_token.txt', 'r').read().strip()}
    elif os.getenv('TEST_LEVEL') == 'TRAVIS':
        access_token_data = {'token': _get_fitbit_access_token()}
        refresh_token_data = {'token': os.getenv('FITBIT_REFRESH_TOKEN')}
    elif access_token:
        access_token_data = {'token': access_token}
        refresh_token_data = {'token': refresh_token}

    default_string = "{} is {}."

    headers = {'clientId': os.environ['FITBIT_CONSUMER_KEY'],
               'Content-length': '999',
               'Content-Type': 'application/x-www-form-urlencoded',
               'Authorization': f"Basic {os.environ['FITBIT_AUTH_HEADER']}"}

    print('\nValidating tokens..')
    valid = True
    for identifier, token in {'Access token': access_token_data, 'Refresh token': refresh_token_data}.items():
        with requests.session() as session:
            r = session.post(
                url='https://api.fitbit.com/1.1/oauth2/introspect',
                data=token,
                headers=headers
            )
            if r.json()['active']:
                print(default_string.format(identifier, 'active'))
                if identifier == 'Access token':
                    access_token_valid = True
                    continue
                else:
                    refresh_token_valid = True
                    continue
            else:
                if identifier != 'Refresh token':
                    print(default_string.format(identifier, 'expired. Please update'))
                    access_token_valid = False
                    continue
                else:
                    print('Refresh token is valid.')
                    refresh_token_valid = True
                    continue

    return access_token_valid, refresh_token_valid
