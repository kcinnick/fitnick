from sqlalchemy.exc import IntegrityError
from sqlalchemy import MetaData, Table, Column, VARCHAR, UniqueConstraint, Numeric, Date

from fitnick.base.base import build_sql_command, get_authorized_client

meta = MetaData()
heart_daily_table = Table(
    'daily',
    meta,
    Column('type', VARCHAR, primary_key=True),
    Column('minutes', Numeric(10, 5)),
    Column('date', Date, nullable=False),
    Column('calories', Numeric(10, 5)),
    UniqueConstraint('type', 'minutes', 'date', 'calories', name='daily_type_minutes_date_calories'),
    schema='heart'
)

heart_daterange_table = Table(
    'daterange',
    meta,
    Column('type', VARCHAR, primary_key=True),
    Column('base_date', Date, nullable=False),
    Column('end_date', Date, nullable=False),
    Column('minutes', Numeric(10, 5)),
    Column('calories', Numeric(10, 5)),
    UniqueConstraint('base_date', 'end_date', 'type', name='daterange_base_date_end_date_type_key'),
    schema='heart'
)


def get_heart_rate_time_series(authorized_client, db_connection, table, config):
    """
    The two time-series based queries supported are documented here:
    https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
    :param authorized_client: An authorized Fitbit client, like the one returned by get_authorized_client.
    :param db_connection: PostgreSQL database connection to /fitbit or /fitbit-test.
    :param config: dict containing the settings that determine what kind of time-series request gets made.
    :return:
    """
    if table.name == 'daterange':
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
            if table.name == 'daterange':
                connection.execute(
                    table.insert(),
                    {"type": heart_range_type,
                     "base_date": config['base_date'],
                     "end_date": config['end_date'],
                     "minutes": details[0],
                     "calories": details[1]}
                )
                continue
            else:
                connection.execute(
                    table.insert(),
                    {"type": heart_range_type,
                     "minutes": details[0],
                     "date": config['base_date'],
                     "calories": details[1]}
                )
                continue
