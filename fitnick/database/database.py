from datetime import timedelta, datetime, date

from fitnick.base.base import create_db_engine
from fitnick.heart_rate.models import heart_daily_table


def build_sql_expression(table, conditions):
    expression = table.select().where(
        table.columns.date == conditions[0]).where(
        table.columns.type == conditions[1]
    )
    return expression


def compare_1d_heart_rate_zone_data(heart_rate_zone, date_str, database, table=heart_daily_table):
    """
    Retrieves & compares today & yesterday's heart rate zone data for the zone specified.
    :param heart_rate_zone: str, Heart rate zone data desired. Options are Cardio, Peak, Fat Burn & Out of Range.
    :param database: str, Database to use for data comparison. Options are fitbit or fitbit-test.
    :param table: sqlalchemy.Table object to retrieve data from.
    :return:
    """
    db_connection = create_db_engine(database=database)

    search_datetime = datetime.strptime(date_str, '%Y-%m-%d')
    yesterday_date_string = (search_datetime - timedelta(days=1)).date()

    with db_connection.connect() as connection:
        today_row = connection.execute(
            table.select().where(table.columns.date == str(search_datetime.date())
                                 ).where(table.columns.type == heart_rate_zone)
        ).fetchone()

        yesterday_row = connection.execute(
            table.select().where(table.columns.date == str(yesterday_date_string)
                                 ).where(table.columns.type == heart_rate_zone)
        ).fetchone()

    print(
        f"You spent {today_row.minutes} minutes in {heart_rate_zone} today, compared to " +
        f"{yesterday_row.minutes} yesterday."
    )

    if heart_rate_zone != 'Out of Range':
        if today_row.minutes < yesterday_row.minutes:
            print('Get moving! That\'s {} minutes less than yesterday!'.format(
                int(yesterday_row.minutes - today_row.minutes)
            ))
        else:
            print('Good work! That\'s {} minutes more than yesterday!'.format(
                int(today_row.minutes - yesterday_row.minutes)
            ))

    return heart_rate_zone, today_row.minutes, yesterday_row.minutes
