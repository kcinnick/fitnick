from datetime import date

from sqlalchemy.dialects.postgresql import insert

from fitnick.database.database import Database
from fitnick.time_series import TimeSeries
from fitnick.heart_rate.models import HeartDaily, heart_daily_table, HeartIntraday, heart_intraday_table


def handle_integrity_error(session, row):
    session.rollback()
    insert_statement = insert(heart_daily_table).values(
        type=row.type,
        minutes=row.minutes,
        date=row.date,
        calories=row.calories,
        resting_heart_rate=row.resting_heart_rate)

    update_statement = insert_statement.on_conflict_do_update(
        constraint='daily_type_date_key',
        set_={
            'type': row.type,
            'minutes': row.minutes,
            'date': row.date,
            'calories': row.calories,
            'resting_heart_rate': row.resting_heart_rate
        })

    session.execute(update_statement)
    session.commit()

    return session


class HeartRateTimeSeries(TimeSeries):
    def __init__(self, config):
        super().__init__(config)
        self.config['resource'] = 'heart'
        self.config['schema'] = 'heart'
        return

    def parse_response(self, data):
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

    @staticmethod
    def parse_intraday_response(date, intraday_response):
        rows = []
        for entry in intraday_response['activities-heart-intraday']['dataset']:
            row = HeartIntraday(date=date, time=entry['time'], value=entry['value'])
            rows.append(row)

        return rows

    def get_total_calories_df(self, show=True):
        from fitnick.database.database import Database
        from pyspark.sql import functions as F

        database = Database(self.config['database'], schema='heart')
        database.create_spark_session()
        df = database.get_df_from_db('daily')
        agg_df = (df.groupBy(F.col('date')).agg(F.sum('calories')).alias('calories')).orderBy('date')

        if show:
            agg_df.show()

        return agg_df

    def get_heart_rate_zone_for_day(self, database: str = 'fitbit', target_date: str = 'today'):
        """
        Retrieves heart rate data for one day only.
        This method should not be used to add batch data - i.e., iterating
        through a list of dates is likely to trigger rate limit errors.
        :param database: Database to insert into.
        :param target_date: Date to retrieve heart rate zone data for.
        :return:
        """
        if target_date != 'today':
            self.config.update({
                'base_date': target_date,
                'end_date': target_date,
                'database': database}
            )
        else:
            today = date.today().strftime('%Y-%m-%d')
            self.config.update({
                'base_date': today,
                'end_date': today,
                'database': database}
            )

        db = Database(database=database, schema=self.config['schema'])
        rows = self.insert_data(db)

        return rows

    def plot_rolling_average(self, days=3):
        import matplotlib.pyplot as plt
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        from fitnick.base.base import get_df_from_db, create_spark_session

        spark_session = create_spark_session()
        df = get_df_from_db(spark_session, 'fitbit', 'heart', 'daily')
        agg_df = df.groupBy(F.col('date')).agg(F.sum('calories')).alias('calories')
        agg_df = agg_df.filter(F.col('date').between(self.config['base_date'], self.config['end_date']))

        window_spec = Window.orderBy(F.col("date")).rowsBetween(-days, 0)
        agg_df = agg_df.withColumn(f'{days}DMA', F.avg("calories.sum(calories)").over(window_spec))
        agg_df = agg_df.toPandas()

        agg_df[f'{days}DMA'] = agg_df[f'{days}DMA'].astype(float)
        agg_df['sum(calories)'] = agg_df['sum(calories)'].astype(float)
        agg_df.plot(
            kind='line',
            x='date',
            y=[f'{days}DMA', 'sum(calories)'],
        )

        plt.show()

        return
