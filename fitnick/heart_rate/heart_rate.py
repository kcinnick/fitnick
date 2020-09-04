from fitnick.base.base import create_db_engine, insert_or_update


def get_heart_rate_zone_time_series(authorized_client, database, table, config):
    """
    The two time-series based queries supported are documented here:
    https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
    :param authorized_client: An authorized Fitbit client, like the one returned by get_authorized_client.
    :param database: Database name - should either be `fitbit` or `fitbit_test`.
    :param table: sqlalchemy.Table object.
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

    heart_rate_zone_series_data = data['activities-heart'][0]['value']['heartRateZones']
    heart_rate_zone_series_data = {i['name']: (i['minutes'], i['caloriesOut']) for i in heart_rate_zone_series_data}
    db_connection = create_db_engine(database=database)

    with db_connection.connect() as connection:
        for heart_range_type, details in heart_rate_zone_series_data.items():
            if table.name == 'daterange':
                payload = {
                    "type": heart_range_type,
                    "base_date": config['base_date'],
                    "end_date": config['end_date'],
                    "minutes": details[0],
                    "calories": details[1]
                }
                insert_or_update(connection, payload, table)
            else:
                payload = {
                    "type": heart_range_type,
                    "minutes": details[0],
                    "date": config['base_date'],
                    "calories": details[1]
                }
                insert_or_update(connection, payload, table)
