"""Main module."""
import fitbit
import os
from sqlalchemy import create_engine


def get_authed_client() -> fitbit.Fitbit:
    """
    Using the defined environment variables for the various Fitbit tokens,
    creates an authorized Fitbit client for a user's credentials.
    :return: Authorized Fitbit client
    """
    authed_client = fitbit.Fitbit(
        os.environ['FITBIT_CONSUMER_KEY'],
        os.environ['FITBIT_CONSUMER_SECRET'],
        os.environ['FITBIT_ACCESS_KEY'],
        os.environ['FITBIT_REFRESH_TOKEN']
    )

    if authed_client.sleep:
        #  If the client isn't authorized, this method will return NoneType
        return authed_client
    else:
        print(
            'Authorization failed - check your refresh token, and ensure'
            + 'your environment variables are set correctly.')
        exit()


def get_heart_rate_time_series_period(authorized_client, db_connection, date='2020-08-26', period='1d'):
    """
    The first of the two time-series based queries documented here:
    https://dev.fitbit.com/build/reference/web-api/heart-rate/#get-heart-rate-time-series
    :param date: The end date of the period specified in the format yyyy-MM-dd or today.
    :param period: The range for which data will be returned. Options are 1d, 7d, 30d, 1w, 1m.
    :return:
    """
    if period == '1d':
        data = authorized_client.time_series(resource='activities/heart', base_date=date, period=period)
        heart_series_data = data['activities-heart'][0]['value']['heartRateZones']
        heart_series_data = {i['name']: (i['minutes'], i['caloriesOut']) for i in heart_series_data}
        with db_connection.connect() as connection:
            for heart_range_type, details in heart_series_data.items():
                sql_string = f"insert into heart.daily(type, minutes, date, calories) values ('{heart_range_type}', {details[0]}, '{date}', {details[1]})"
                connection.execute(sql_string)
    else:
        raise NotImplementedError

    return


def main():
    authorized_client = get_authed_client()
    db_connection = create_engine(f"postgres+psycopg2://{os.environ['postgres_username']}:{os.environ['postgres_password']}@{os.environ['postgress_ip']}:5432/fitbit")
    get_heart_rate_time_series_period(authorized_client, db_connection)


if __name__ == '__main__':
    main()
