"""Main module."""
import os

import fitbit
from pyspark.sql import SparkSession
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
                'clientId': os.environ['FITBIT_CONSUMER_KEY'],
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f"Basic {os.environ['FITBIT_AUTH_HEADER']}"}
        )
        os.environ['FITBIT_ACCESS_KEY'] = r.json()['access_token']
        os.environ['FITBIT_REFRESH_TOKEN'] = r.json()['refresh_token']
        print(r.json())

    return


def get_df_from_db(spark_session, database, schema, table):
    """
    Retrieves a PySpark dataframe containing all of the data in the specified table.
    :param spark_session: Existing SparkSession object
    :param database: str, name of database
    :param schema: str, name of schema
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
        table=table
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


def insert_or_update(connection, payload, table):
    try:
        connection.execute(
            table.insert(),
            payload
        )
    except IntegrityError:
        try:
            connection.execute(
                table.update(),
                payload
            )
        except IntegrityError:
            print('Data already exists in DB. Continuing.\n')
            return
