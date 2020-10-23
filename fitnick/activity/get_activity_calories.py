from fitnick.activity.activity import Activity
from fitnick.database.database import Database


def gather_calories_for_day(day='2020-10-22'):
    activity = Activity(
        config={
            'database': 'fitbit',
            'base_date': day
        }
    )
    raw_calorie_summary = activity.query_calorie_summary()
    row = activity.parse_calorie_summary(activity.config['base_date'], raw_calorie_summary)
    database = Database('fitbit', 'schema')
    activity.insert_calorie_data(database, row)

    return


def main():
    gather_calories_for_day()


if __name__ == '__main__':
    main()
