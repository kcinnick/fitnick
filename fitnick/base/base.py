"""Main module."""
from datetime import datetime
import os

import fitbit


def get_authorized_client() -> fitbit.Fitbit:
    """
    Using the defined environment variables for the various Fitbit tokens,
    creates an authorized Fitbit client for a user's credentials.
    :return: Authorized Fitbit client
    """
    authorized_client = fitbit.Fitbit(
        os.environ['FITBIT_CONSUMER_KEY'],
        os.environ['FITBIT_CONSUMER_SECRET'],
        os.environ['FITBIT_ACCESS_KEY'],
        os.environ['FITBIT_REFRESH_TOKEN']
    )

    if authorized_client.sleep:
        #  If the client isn't authorized, this method will return NoneType
        return authorized_client
    else:
        print(
            'Authorization failed - check your refresh token, and ensure'
            + 'your environment variables are set correctly.')
        exit()


def refresh_authorized_client():
    import requests
    with requests.session() as session:
        data = {'grant_type': 'refresh_token',
                'refresh_token': os.environ['FITBIT_REFRESH_TOKEN']}
        r = session.post(
            url='https://api.fitbit.com/oauth2/token',
            data=data,
            headers={
                'clientId': os.environ['FITBIT_CONSUMER_KEY'],
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f"Basic {os.environ['FITBIT_AUTH_HEADER']}"}
        )
        os.environ['FITBIT_ACCESS_KEY'] = r.json()['access_token']
        os.environ['FITBIT_REFRESH_TOKEN'] = r.json()['refresh_token']
    return


def check_date(date):
    """
    Prevents a user from entering incomplete data for the current day.
    Returns false if the provided date str == today's date str.
    :param date: str
    :return: bool
    """
    if date == datetime.today().strftime('%Y-%m-%d'):
        print('Can\'t insert data for today - it\'s not over yet! Exiting.\n')
        return False
    return True


def build_sql_command(config, type, data):
    sql_string = f"insert into {config['database']}.{config['table']} ("
    sql_string += ', '.join(config['columns'])
    sql_string += f") values "
    if type == 'heart_rate_time_series_daterange':
        sql_string += f"{config['base_date'], config['end_date'], data[0], data[1][0], data[1][1]}"
    elif type == 'heart_rate_time_series_period':
        sql_string += f"{data[0], data[1][0], config['base_date'], data[1][1]}"
    else:
        raise NotImplementedError('Unsupported type: ', type)
    return sql_string
