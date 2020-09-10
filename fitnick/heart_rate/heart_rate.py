from datetime import date, timedelta

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm.exc import FlushError
from tqdm import tqdm

from fitnick.base.base import create_db_engine, get_authorized_client
from fitnick.heart_rate.models import HeartDaily


def rollback_and_commit(session, row):
    session.rollback()
    session.add(row)
    session.commit()


def update_old_rows(session, row):
    session.rollback()
    session.commit()
    rows = session.query(HeartDaily).filter_by(date=row.date).filter_by(type=row.type).all()
    for old_row in rows:
        if old_row.minutes != row.minutes:  # if there's a discrepancy, delete the old row & add the new one
            session.delete(old_row)
            session.commit()
            session.add(row)
            session.commit()
        else:
            continue
    return


def query_heart_rate_zone_time_series(authorized_client, config):
    """
    The two time-series based queries supported are documented here:
    https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
    :param authorized_client: An authorized Fitbit client, like the one returned by get_authorized_client.
    :param config: dict containing the settings that determine what kind of time-series request gets made.
    :return:
    """
    try:
        assert len(config['base_date'].split('-')[0]) == 4
    except AssertionError:
        print('Dates must be formatted as YYYY-MM-DD. Exiting.')
        exit()

    if config.get('end_date'):
        data = authorized_client.time_series(
            resource='activities/heart',
            base_date=config['base_date'],
            end_date=config['end_date']
        )
    else:
        # we're assuming that if it's not a daterange search, it's a period search.
        # period searches look backwards for a base_date - i.e., a base_date of
        # 2020-09-02 will cover 2020-08-27 to 2020-09-02
        data = authorized_client.time_series(
            resource='activities/heart',
            base_date=config['base_date'],
            period=config['period']
        )

    return data


def parse_response(data):
    rows = []
    for day in data['activities-heart']:
        date = day['dateTime']
        try:
            resting_heart_rate = day['value']['restingHeartRate']
        except KeyError:
            resting_heart_rate = 0
        for heart_rate_zone in day['value']['heartRateZones']:
            row = HeartDaily(
                type=heart_rate_zone['name'],
                minutes=heart_rate_zone.get('minutes', 0),
                date=date,
                calories=heart_rate_zone.get('caloriesOut', 0),
                resting_heart_rate=resting_heart_rate
            )
            rows.append(row)

    return rows


def insert_heart_rate_time_series_data(config, raise_exception=False):
    authorized_client = get_authorized_client()
    data = query_heart_rate_zone_time_series(authorized_client, config)
    parsed_rows = parse_response(data)
    db = create_db_engine(config['database'])
    session = sessionmaker(bind=db)
    session = session()
    for row in tqdm(parsed_rows):
        session.add(row)

    try:
        session.commit()
    except IntegrityError as e:
        if raise_exception:
            raise e
        else:
            pass

    return parsed_rows


def get_today_heart_rate_time_series_data(database):
    rows = insert_heart_rate_time_series_data(config={
        'database': database,
        'base_date': date.today().strftime('%Y-%m-%d'),
        'period': '1d'
    })

    return rows


def get_heart_rate_zone_for_day(database, date):
    # add a check to only get this if we don't already have it
    rows = insert_heart_rate_time_series_data(config={
        'database': database,
        'base_date': date,
        'period': '1d'
    })

    return rows


def backfill(database: str, period: int = 90):
    """
    Backfills a database from the current day.
    Example: if run on 2020-09-06 with period=90, the database will populate for 2020-06-08 - 2020-09-06
    :param database:
    :param period:
    :return:
    """
    backfill_date = date.today() - timedelta(days=period)
    for day in range(period):
        backfill_day = (backfill_date + timedelta(days=day)).strftime('%Y-%m-%d')
        print(f'Retrieving heart rate zone data for {backfill_day}.\n')
        get_heart_rate_zone_for_day(database, date=backfill_day)

    return

#backfill('fitbit', period=30)
