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
    """
    Currently serves as the endpoint for the get_heart_rate_zone methods, even
    though it's currently set to the index page
    :return:
    """
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    statement = heart_daily_table.select().where(heart_daily_table.columns.date == str(date.today()))
    rows = [i for i in Database(database='fitbit', schema='heart').engine.execute(statement)]
    #  retrieve rows for today already in database, if there are none then get rows via fitnick
    if len(rows) == 0:
        rows = heart_rate_zone.get_heart_rate_zone_for_day(database='fitbit')

    rows = [i for i in rows]

    form = DateForm(request.form)

    month_options = [i for i in range(1, 13)]
    day_options = [i for i in range(1, 32)]
    year_options = range(2020, 2021)

    if request.method == 'GET':
        return render_template(
            "index.html",
            rows=rows,
            form=form,
            month_options=month_options,
            day_options=day_options,
            year_options=year_options
        )


def set_search_date(request, search_date):
    if request.method == 'POST':
        # collect search date information from the dropdown forms if they're all supplied.
        if all([request.form.get('month_options'), request.form.get('day_options'), request.form.get('year_options')]):
            search_date = '-'.join(
                [f"{request.form['year_options']}",
                 f"{request.form['month_options']}".zfill(2),
                 f"{request.form['day_options']}".zfill(2)])
        else:
            # use the search_date value we set in lines 59-62
            pass

    return search_date


@app.route("/get_heart_rate_zone_today", methods=['GET', 'POST'])
def get_heart_rate_zone_today():
    """
    Endpoint for getting heart rate zone data from the FitBit API.
    :return:
    """
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    form = DateForm(request.form)
    value = 'Updated heart rate zone data for {}.'

    if form.date._value():  # set search_date, default to today if none supplied
        search_date = form.date._value()
    else:
        search_date = str(date.today())

    if request.method == 'POST':
        # collect search date information from the dropdown forms if they're all supplied.
        search_date = set_search_date(request, search_date)
        rows = heart_rate_zone.get_heart_rate_zone_for_day(
            database='fitbit',
            target_date=search_date)
        rows = [i for i in rows]
    else:  # no date supplied, just return data for today.
        heart_rate_zone.config = {'base_date': date.today(), 'period': '1d'}
        statement = heart_daily_table.select().where(heart_daily_table.columns.date == str(date.today()))
        rows = Database(database='fitbit', schema='heart').engine.execute(statement)

    return render_template(
        "index.html",
        value=value.format(search_date),
        rows=rows,
        form=form,
        month_options=range(1, 13),
        day_options=[i for i in range(1, 32)],
        year_options=range(2020, 2021)
    )


@app.route("/get_activity_today", methods=['GET', 'POST'])
def get_activity_today():
    """
    Endpoint for getting activity data for a given date from the FitBit API.
    :return:
    """

    activity = Activity(config={'database': 'fitbit'})
    form = DateForm(request.form)
    search_date = form.date._value()

    if request.method == 'POST':
        search_date = set_search_date(request, search_date)
        row = activity.get_calories_for_day(day=search_date)
        value = 'Updated activity data for {}.'.format(search_date)
    else:
        row = []
        value = ''

    return render_template(
        'activity.html',
        form=form,
        row=row,
        value=value,
        month_options=range(1, 13),
        day_options=[i for i in range(1, 32)],
        year_options=range(2020, 2021)
    )


@app.route('/<page_name>')
def other_page(page_name):
    """
    Stand-in endpoint for any undefined URL.
    :param page_name:
    :return:
    """
    response = make_response(f'The page named {page_name} does not exist.', 404)
    return response


if __name__ == '__main__':
    app.run(debug=True)
