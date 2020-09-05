from sqlalchemy.exc import IntegrityError

from sqlalchemy.orm import sessionmaker
from fitnick.heart_rate.models import HeartDaily


def get_heart_rate_zone_time_series(authorized_client, engine, config):
    """
    The two time-series based queries supported are documented here:
    https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
    :param authorized_client: An authorized Fitbit client, like the one returned by get_authorized_client.
    :param database: Database name - should either be `fitbit` or `fitbit_test`.
    :param table: sqlalchemy.Table object.
    :param config: dict containing the settings that determine what kind of time-series request gets made.
    :return:
    """
    if config.get('end_date'):
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

    session = sessionmaker()
    session.configure(bind=engine)
    session = session()

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
            try:
                session.add(row)
                session.commit()
                continue
            except IntegrityError:
                session.rollback()
                rows = session.query(HeartDaily).filter_by(date=date).filter_by(type=heart_rate_zone['name']).all()
                for old_row in rows:
                    if old_row.minutes != row.minutes:  # if there's a discrepancy, delete the old row & add the new one
                        session.delete(old_row)
                        session.commit()
                        session.add(row)
                        session.commit()
                    else:
                        continue
