import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


def get_calories_for_month(database='fitbit'):
    # aggregate sum calories for a certain month
    db_connection = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USERNAME']}:" +
        f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_IP']}" +
        f":5432/{database}", poolclass=NullPool
    )
    df = pd.read_sql_table(
        con=db_connection,
        table_name='calories',
        schema='activity'
    )
    df = df.groupby(df.date.dt.month).agg(['sum'])
    print(df)

    return df


def main():
    get_calories_for_month()


if __name__ == '__main__':
    main()
