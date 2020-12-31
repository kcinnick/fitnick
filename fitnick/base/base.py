"""Main module."""
import os

import fitbit
from pyspark.sql import SparkSession
from sqlalchemy import create_engine


def get_authorized_client() -> fitbit.Fitbit:
    """
    Using the defined environment variables for the various Fitbit tokens,
    creates an authorized Fitbit client for a user's credentials.
    :return: Authorized Fitbit client
    """
    if os.getenv('TEST_LEVEL') == 'local':
        with open(r'C:\Users\devni\PycharmProjects\fitnick\fitnick\base\fitbit_access_key.txt', 'r') as f:
            access_key = f.read().strip()

        with open(r'C:\Users\devni\PycharmProjects\fitnick\fitnick\base\fitbit_refresh_token.txt', 'r') as f:
            refresh_token = f.read().strip()
    else:
        access_key = os.getenv('FITBIT_ACCESS_KEY')
        refresh_token = os.getenv('FITBIT_REFRESH_TOKEN')

    authorized_client = fitbit.Fitbit(
        os.environ['FITBIT_CONSUMER_KEY'],
        os.environ['FITBIT_CONSUMER_SECRET'],
        access_key,
        refresh_token
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
        access_token_data = {'token': os.getenv('FITBIT_ACCESS_KEY')}
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
