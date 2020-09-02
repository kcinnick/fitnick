from datetime import timedelta
from datetime import date

from sqlalchemy.sql import and_

from fitnick.base.base import get_authorized_client, create_db_engine
from fitnick.models import heart_daterange_table, heart_daily_table


def build_sql_expression(table, conditions):
    expression = table.select().where(
        table.columns.date == conditions[0]).where(
        table.columns.type == conditions[1]
    )
    return expression


def compare_1d_heart_rate_zone_data(heart_rate_zone='Cardio', database='fitbit', table=heart_daily_table):
    """
    Retrieves & compares today & yesterday's heart rate zone data for the zone specified.
    :param heart_rate_zone: str, Heart rate zone data desired. Options are Cardio, Peak, Fat Burn & Out of Range.
    :param database: str, Database to use for data comparison. Options are fitbit or fitbit-test.
    :param table: sqlalchemy.Table object to retrieve data from.
    :return:
    """
    db_connection = create_db_engine(database=database)

    today_date_string = date.today()
    yesterday_date_string = date.today() - timedelta(days=1)

    minutes_in_zone_today_expression = build_sql_expression(table, [today_date_string, heart_rate_zone])
    minutes_in_zone_yesterday_expression = build_sql_expression(table, [yesterday_date_string, heart_rate_zone])

    with db_connection.connect() as connection:
        minutes_in_zone_today = [i[1] for i in connection.execute(minutes_in_zone_today_expression)][0]
        minutes_in_zone_yesterday = [i[1] for i in connection.execute(minutes_in_zone_yesterday_expression)][0]

    print(
        f"You spent {minutes_in_zone_today} minutes in {heart_rate_zone} today, compared to " +
        f"{minutes_in_zone_yesterday} yesterday."
    )

    if heart_rate_zone != 'Out of Range':
        if minutes_in_zone_today < minutes_in_zone_yesterday:
            print('Get moving! That\'s {} minutes less than yesterday!'.format(
                int(minutes_in_zone_yesterday - minutes_in_zone_today)
            ))
        else:
            print('Good work! That\'s {} minutes more than yesterday!'.format(
                int(minutes_in_zone_today - minutes_in_zone_yesterday)
            ))

    return


def main():
    compare_1d_heart_rate_zone_data('Cardio', heart_daily_table)


if __name__ == '__main__':
    main()
