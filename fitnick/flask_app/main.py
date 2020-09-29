from datetime import date

from flask import Flask, make_response, render_template, request
from fitnick.database.database import Database
from fitnick.heart_rate.heart_rate import HeartRateTimeSeries
from fitnick.heart_rate.models import heart_daily_table

app = Flask(__name__)


@app.route("/", methods=['GET'])
def index():
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    rows = heart_rate_zone.get_heart_rate_zone_for_day(database='fitbit')
    return render_template(
        "index.html", value='Here\'s the latest heart rate data in the database.',
        rows=[row for row in rows if row.date == str(date.today())]
    )


@app.route("/get_heart_rate_zone_today", methods=['GET', 'POST'])
def get_heart_rate_zone_today():
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    if request.method == 'POST':
        print("Get heart rate zone method.")
        rows = heart_rate_zone.get_heart_rate_zone_for_day(database='fitbit')
        rows = [row for row in rows if row.date == str(date.today())]
        return render_template(
            "index.html", value='Today\'s heart rate data was placed in the database!',
            rows=[row for row in rows if row.date == str(date.today())]
        )
    else:
        heart_rate_zone.config = {'base_date': date.today(), 'period': '1d'}
        statement = heart_daily_table.select().where(heart_daily_table.columns.date == str(date.today()))
        rows = Database(database='fitbit', schema='heart').engine.execute(statement)
        return render_template(
            "index.html", value='Here\'s the latest heart rate data in the database.',
            rows=[row for row in rows if row.date == str(date.today())]
        )


@app.route('/<page_name>')
def other_page(page_name):
    response = make_response(f'The page named {page_name} does not exist.', 404)
    return response


if __name__ == '__main__':
    app.run(debug=True)
