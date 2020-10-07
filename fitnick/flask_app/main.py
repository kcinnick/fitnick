from datetime import date
import os

from flask import Flask, make_response, render_template, request
from flask_wtf import FlaskForm
from wtforms import StringField

from fitnick.database.database import Database
from fitnick.heart_rate.heart_rate import HeartRateTimeSeries
from fitnick.heart_rate.models import heart_daily_table

app = Flask(__name__)
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY


class DateForm(FlaskForm):
    date = StringField('date')


@app.route("/", methods=['GET'])
def index():
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    rows = heart_rate_zone.get_heart_rate_zone_for_day(database='fitbit')
    form = DateForm(request.form)
    return render_template(
        "index.html", value='Here\'s the latest heart rate data in the database.',
        rows=rows,
        form=form
    )


@app.route("/get_heart_rate_zone_today", methods=['GET', 'POST'])
def get_heart_rate_zone_today():
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    form = DateForm(request.form)

    if request.method == 'POST':
        print("Get heart rate zone method.")
        rows = heart_rate_zone.get_heart_rate_zone_for_day(database='fitbit')
        return render_template(
            "index.html", value='Today\'s heart rate data was placed in the database!',
            rows=rows,
            form=form
        )
    else:
        heart_rate_zone.config = {'base_date': date.today(), 'period': '1d'}
        statement = heart_daily_table.select().where(heart_daily_table.columns.date == str(date.today()))
        rows = Database(database='fitbit', schema='heart').engine.execute(statement)
        return render_template(
            "index.html", value='Here\'s the latest heart rate data in the database.',
            rows=rows,
            form=form
        )


@app.route("/get_heart_rate_zone_for_date", methods=['GET', 'POST'])
def get_heart_rate_zone_date():
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    form = DateForm(request.form)
    rows = []

    if request.method == 'POST':
        rows = heart_rate_zone.get_heart_rate_zone_for_day(database='fitbit', target_date=form.date._value())

    return render_template('index.html', form=form, rows=rows)


@app.route('/<page_name>')
def other_page(page_name):
    response = make_response(f'The page named {page_name} does not exist.', 404)
    return response


if __name__ == '__main__':
    app.run(debug=True)
