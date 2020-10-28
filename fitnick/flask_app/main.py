from datetime import date
import os

from flask import Flask, make_response, render_template, request
from flask_wtf import FlaskForm
from sqlalchemy.orm.exc import DetachedInstanceError
from wtforms import StringField

from fitnick.activity.activity import Activity
from fitnick.database.database import Database
from fitnick.heart_rate.time_series import HeartRateTimeSeries
from fitnick.heart_rate.models import heart_daily_table

app = Flask(__name__)
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY


class DateForm(FlaskForm):
    date = StringField('date')


@app.route("/", methods=['GET'])
def index():
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    statement = heart_daily_table.select().where(heart_daily_table.columns.date == str(date.today()))
    rows = [i for i in Database(database='fitbit', schema='heart').engine.execute(statement)]
    if len(rows) == 0:
        rows = heart_rate_zone.get_heart_rate_zone_for_day(database='fitbit')
    form = DateForm(request.form)
    if request.method == 'GET':
        return render_template(
            "index.html",
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
        print('70P')
        try:
            rows = [i for i in heart_rate_zone.get_heart_rate_zone_for_day(
                database='fitbit', target_date=form.date._value())]
        except AttributeError as e:  # the date regex check failed
            return render_template(
                "index.html",
                value="""There was an error: {}.        
                Check to make sure you entered a properly formatted date.""".format(e),
                rows=rows,
                form=form
            )
    while True:
        try:
            print('83G')
            return render_template('index.html', form=form, rows=rows)
        except DetachedInstanceError:
            continue


@app.route("/activity", methods=['GET', 'POST'])
def activity():
    activity = Activity(config={'database': 'fitbit'})
    form = DateForm(request.form)
    rows = []

    if request.method == 'POST':
        rows = [i for i in activity.gather_calories_for_day(day='2020-10-26')]
    else:
        print('GET')

    return render_template('activity.html', form=None)


@app.route('/<page_name>')
def other_page(page_name):
    response = make_response(f'The page named {page_name} does not exist.', 404)
    return response


if __name__ == '__main__':
    app.run(debug=True)
