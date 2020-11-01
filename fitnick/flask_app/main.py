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
        rows = [i for i in rows]
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
    value = 'Updated heart rate zone data for {}.'

    if request.method == 'POST':
        rows = heart_rate_zone.get_heart_rate_zone_for_day(
            database='fitbit',
            target_date=form.date._value()
        )
        rows = [i for i in rows]

        return render_template(
            "index.html",
            value=value.format(form.date._value()),
            rows=rows,
            form=form
        )

    else:
        heart_rate_zone.config = {'base_date': date.today(), 'period': '1d'}
        statement = heart_daily_table.select().where(heart_daily_table.columns.date == str(date.today()))
        rows = Database(database='fitbit', schema='heart').engine.execute(statement)
        return render_template(
            "index.html",
            value=value.format(date.today()),
            rows=rows,
            form=form
        )


@app.route("/get_activity_today", methods=['GET', 'POST'])
def get_activity_today():
    activity = Activity(config={'database': 'fitbit'})
    form = DateForm(request.form)
    if request.method == 'POST':
        row = activity.get_calories_for_day(day=form.date._value())
        value = 'Updated activity data for {}.'.format(form.date._value())
    else:
        row = []
        value = ''

    return render_template(
        'activity.html',
        form=form,
        row=row,
        value=value
    )


@app.route('/<page_name>')
def other_page(page_name):
    response = make_response(f'The page named {page_name} does not exist.', 404)
    return response


if __name__ == '__main__':
    app.run(debug=True)
