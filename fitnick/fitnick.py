"""Main module."""
from datetime import datetime
import os

import fitbit
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError


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
                'clientId': '22BWR3',
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


def get_heart_rate_time_series(authorized_client, db_connection, config):
    """
    The two time-series based queries supported are documented here:
    https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
    :param authorized_client: An authorized Fitbit client, like the one returned by get_authorized_client.
    :param db_connection: PostgreSQL database connection to /fitbit or /fitbit-test.
    :param config: dict containing the settings that determine what kind of time-series request gets made.
    :return:
    """
    if 'end_date' in config.keys():
        data = authorized_client.time_series(
            resource='activities/heart',
            base_date=config['base_date'],
            end_date=config['end_date']
        )
    else:  # we're assuming that if it's not a daterange search, it's a period search.
        data = authorized_client.time_series(
            resource='activities/heart',
            base_date=config['base_date'],
            period=config['period']
        )

    try:
        assert len(config['base_date'].split('-')[0]) == 4
    except AssertionError:
        print('Dates must be formatted as YYYY-MM-DD. Exiting.')
        exit()

    heart_series_data = data['activities-heart'][0]['value']['heartRateZones']
    heart_series_data = {i['name']: (i['minutes'], i['caloriesOut']) for i in heart_series_data}
    with db_connection.connect() as connection:
        for heart_range_type, details in heart_series_data.items():
            if 'end_date' in config.keys():
                sql_string = build_sql_command(config, type='heart_rate_time_series_daterange', data=[heart_range_type, details])
                try:
                    connection.execute(sql_string)
                except IntegrityError: # data already exists for this date.
                    print('Data already exists in database for this date. Exiting.\n')
                    return
            else:
                sql_string = build_sql_command(config, type='heart_rate_time_series_period', data=[heart_range_type, details])
                try:
                    connection.execute(sql_string)
                except IntegrityError: # data already exists for this date.
                    print('Data already exists in database for this date. Exiting.\n')
                    return
    return


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


def main():
    authorized_client = get_authorized_client()
    db_connection = create_engine(f"postgres+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}:5432/fitbit_test")
    config = {'database': 'heart',
              'table': 'daily',
              'base_date': '2020-08-26',
              'period': '1d',
              'columns': ['type', 'minutes', 'date', 'calories']
              }
    get_heart_rate_time_series(authorized_client, db_connection, config)
    #  get_heart_rate_time_series(authorized_client, db_connection, config)


if __name__ == '__main__':
    main()

