from datetime import date
import os

from flask import Flask, make_response, render_template, request
from flask_wtf import FlaskForm

from wtforms import StringField

from fitnick.activity.activity import Activity
from fitnick.database.database import Database
from fitnick.heart_rate.time_series import HeartRateTimeSeries
from fitnick.heart_rate.models import heart_daily_table

app = Flask(__name__)
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

month_options = [i for i in range(1, 13)]
day_options = [i for i in range(1, 32)]
year_options = range(2020, 2021)


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

    if request.method == 'GET':
        return render_template(
            template_name_or_list="index.html",
            rows=rows,
            form=form,
            month_options=month_options,
            day_options=day_options,
            year_options=year_options
        )


@app.route("/get_heart_rate_zone_today", methods=['GET', 'POST'])
def get_heart_rate_zone_today():
    """
    Endpoint for getting heart rate zone data from the FitBit API.
    :return:
    """
    heart_rate_zone = HeartRateTimeSeries(config={'database': 'fitbit'})
    form = DateForm(request.form)
    value = 'Updated heart rate zone data for {}.'

    search_date = str(date.today())

    if request.method == 'POST':
        if request.form.get('year_options'):  # set search_date, default to today if none supplied
            search_date = f"{request.form['year_options']}-{request.form['month_options']}-{request.form['day_options']}"
        # collect search date information from the dropdown forms if they're all supplied.
        rows = heart_rate_zone.get_heart_rate_zone_for_day(
            database='fitbit',
            target_date=search_date)
        rows = [i for i in rows]
    else:  # request.method == 'GET'
        # no date supplied, just return data for today.
        heart_rate_zone.config = {'base_date': date.today(), 'period': '1d'}
        statement = heart_daily_table.select().where(heart_daily_table.columns.date == str(date.today()))
        rows = Database(database='fitbit', schema='heart').engine.execute(statement)

    return render_template(
        template_name_or_list="index.html",
        value=value.format(search_date),
        rows=rows,
        form=form,
        month_options=month_options,
        day_options=day_options,
        year_options=year_options
    )


@app.route("/get_activity_today", methods=['GET', 'POST'])
def get_activity_today():
    """
    Endpoint for getting activity data for a given date from the FitBit API.
    :return:
    """

    activity = Activity(config={'database': 'fitbit'})
    form = DateForm(request.form)

    if request.method == 'POST':
        search_date = f"{request.form['year_options']}-{request.form['month_options']}-{request.form['day_options']}"
        row = activity.get_calories_for_day(day=search_date)
        value = 'Updated activity data for {}.'.format(search_date)
    else:
        row, value = {}, ''

    return render_template(
        template_name_or_list='activity.html',
        form=form,
        row=row,
        value=value,
        month_options=month_options,
        day_options=day_options,
        year_options=year_options
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
