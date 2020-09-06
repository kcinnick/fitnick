import os
from datetime import date

from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import FlushError

from tqdm import tqdm

from fitnick.base.base import create_db_engine, get_authorized_client
from fitnick.heart_rate.models import HeartDaily, heart_daily_table


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


def upload_to_db(session, row):
    try:
        session.flush()
        session.add(row)
        session.commit()
    except IntegrityError:
        update_old_rows(session, row)
    except FlushError:
        session.flush()
        session.rollback()
        session.add(row)
        session.commit()


def insert_heart_rate_time_series_data(config):
    authorized_client = get_authorized_client()
    data = query_heart_rate_zone_time_series(authorized_client, config)
    parsed_rows = parse_response(data)
    db = create_db_engine(config['database'])
    for row in tqdm(parsed_rows):
        session = sessionmaker()
        session.configure(bind=db)
        session = session()
        upload_to_db(session, row=row)
        session.close()

    return


def get_today_heart_rate_time_series_data(database):
    insert_heart_rate_time_series_data(config={
        'database': database,
        'base_date': date.today().strftime('%Y-%m-%d'),
        'period': '1d'
    })
