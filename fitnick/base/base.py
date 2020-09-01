"""Main module."""
from datetime import datetime
import os

import fitbit
from sqlalchemy import create_engine


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


def create_db_engine(database='fitbit'):
    db_connection = create_engine(
        f"postgres+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
        f":5432/{database}"
    )

    return db_connection
