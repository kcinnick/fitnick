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
    previous_date_string = (search_datetime - timedelta(days=1)).date()

    with db_connection.connect() as connection:
        day_row = connection.execute(
            table.select().where(table.columns.date == str(search_datetime.date())
                                 ).where(table.columns.type == heart_rate_zone)
        ).fetchone()

        previous_day_row = connection.execute(
            table.select().where(table.columns.date == str(previous_date_string)
                                 ).where(table.columns.type == heart_rate_zone)
        ).fetchone()

    print(
        f"You spent {day_row.minutes} minutes in {heart_rate_zone} today, compared to " +
        f"{previous_day_row.minutes} yesterday."
    )

    if heart_rate_zone != 'Out of Range':
        if day_row.minutes < previous_day_row.minutes:
            print(
                f'Get moving! That\'s {previous_day_row.minutes - day_row.minutes} minutes less than yesterday!'
            )
        else:
            print('Good work! That\'s {} minutes more than yesterday!'.format(
                int(day_row.minutes - previous_day_row.minutes)
            ))

    return heart_rate_zone, day_row.minutes, previous_day_row.minutes
