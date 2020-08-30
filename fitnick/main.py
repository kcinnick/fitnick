"""Main module."""
from datetime import datetime
import os

import fitbit
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

from fitnick.base.base import get_authorized_client
from fitnick.heart_rate.heart_rate import get_heart_rate_time_series


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

