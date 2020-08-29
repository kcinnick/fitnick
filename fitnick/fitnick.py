"""Main module."""
from datetime import datetime
import os

import fitbit
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError


def get_authed_client() -> fitbit.Fitbit:
    """
    Using the defined environment variables for the various Fitbit tokens,
    creates an authorized Fitbit client for a user's credentials.
    :return: Authorized Fitbit client
    """
    authed_client = fitbit.Fitbit(
        os.environ['FITBIT_CONSUMER_KEY'],
        os.environ['FITBIT_CONSUMER_SECRET'],
        os.environ['FITBIT_ACCESS_KEY'],
        os.environ['FITBIT_REFRESH_TOKEN']
    )

    if authed_client.sleep:
        #  If the client isn't authorized, this method will return NoneType
        return authed_client
    else:
        print(
            'Authorization failed - check your refresh token, and ensure'
            + 'your environment variables are set correctly.')
        exit()


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


def get_heart_rate_time_series_period(authorized_client, db_connection, date='2020-08-26', period='1d'):
    """
    The first of the two time-series based queries documented here:
    https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
    :param date: The end date of the period specified in the format yyyy-MM-dd or today.
    :param period: The range for which data will be returned. Options are 1d, 7d, 30d, 1w, 1m.
    :return:
    """
    date_dict = {
        '1d': 'daily',
        '1m': 'monthly',
        '1w': 'weekly',
        '7d': 'weekly',
        '30d': 'monthly'
    }
    data = authorized_client.time_series(resource='activities/heart', base_date=date, period=period)
    heart_series_data = data['activities-heart'][0]['value']['heartRateZones']
    heart_series_data = {i['name']: (i['minutes'], i['caloriesOut']) for i in heart_series_data}
    with db_connection.connect() as connection:
        for heart_range_type, details in heart_series_data.items():
            if not check_date(date):
                return
            sql_string = f"insert into heart.{date_dict[period]}(type, minutes, date, calories) values ('{heart_range_type}', {details[0]}, '{date}', {details[1]})"
            try:
                connection.execute(sql_string)
            except IntegrityError: # data already exists for this date.
                print('Data already exists in database for this date. Exiting.\n')
                return


def main():
    authorized_client = get_authed_client()
    db_connection = create_engine(f"postgres+psycopg2://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}:5432/fitbit")
    get_heart_rate_time_series_period(authorized_client, db_connection, date='2020-08-29', period='1d')


if __name__ == '__main__':
    main()
