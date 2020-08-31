from sqlalchemy.exc import IntegrityError

from fitnick.base.base import build_sql_command, get_authorized_client


def get_heart_rate_time_series(authorized_client, db_connection, config):
    """
    The two time-series based queries supported are documented here:
    https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
    :param authorized_client: An authorized Fitbit client, like the one returned by get_authorized_client.
    :param db_connection: PostgreSQL database connection to /fitbit or /fitbit-test.
    :param config: dict containing the settings that determine what kind of time-series request gets made.
    :return:
    """
    if 'end_date' in config.keys():
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
            if 'end_date' in config.keys():
                sql_string = build_sql_command(
                    config,
                    type='heart_rate_time_series_daterange',
                    data=[heart_range_type, details]
                )
            else:
                sql_string = build_sql_command(
                    config,
                    type='heart_rate_time_series_period',
                    data=[heart_range_type, details]
                )
            try:
                connection.execute(sql_string)
            except IntegrityError:  # data already exists for this date.
                print('Data already exists in database for this date. Moving on.\n')
    return
