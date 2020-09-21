import datetime
import os

import pytest

from fitnick.database.database import Database
from fitnick.sleep.sleep import Sleep, sleep_summary_table, parse_summary_response

EXPECTED_API_RESPONSE = {
    'sleep': [
        {'dateOfSleep': '2020-09-05', 'duration': 32400000, 'efficiency': 89, 'endTime': '2020-09-05T11:34:30.000',
         'infoCode': 0, 'isMainSleep': True, 'levels': {
            'data': [{'dateTime': '2020-09-05T02:34:30.000', 'level': 'light', 'seconds': 60},
                     {'dateTime': '2020-09-05T02:35:30.000', 'level': 'wake', 'seconds': 420},
                     {'dateTime': '2020-09-05T02:42:30.000', 'level': 'light', 'seconds': 840},
                     {'dateTime': '2020-09-05T02:56:30.000', 'level': 'deep', 'seconds': 870},
                     {'dateTime': '2020-09-05T03:11:00.000', 'level': 'light', 'seconds': 240},
                     {'dateTime': '2020-09-05T03:15:00.000', 'level': 'wake', 'seconds': 240},
                     {'dateTime': '2020-09-05T03:19:00.000', 'level': 'light', 'seconds': 930},
                     {'dateTime': '2020-09-05T03:34:30.000', 'level': 'deep', 'seconds': 660},
                     {'dateTime': '2020-09-05T03:45:30.000', 'level': 'light', 'seconds': 120},
                     {'dateTime': '2020-09-05T03:47:30.000', 'level': 'wake', 'seconds': 300},
                     {'dateTime': '2020-09-05T03:52:30.000', 'level': 'light', 'seconds': 150},
                     {'dateTime': '2020-09-05T03:55:00.000', 'level': 'deep', 'seconds': 1470},
                     {'dateTime': '2020-09-05T04:19:30.000', 'level': 'light', 'seconds': 60},
                     {'dateTime': '2020-09-05T04:20:30.000', 'level': 'wake', 'seconds': 510},
                     {'dateTime': '2020-09-05T04:29:00.000', 'level': 'rem', 'seconds': 2130},
                     {'dateTime': '2020-09-05T05:04:30.000', 'level': 'light', 'seconds': 3090},
                     {'dateTime': '2020-09-05T05:56:00.000', 'level': 'deep', 'seconds': 420},
                     {'dateTime': '2020-09-05T06:03:00.000', 'level': 'light', 'seconds': 1320},
                     {'dateTime': '2020-09-05T06:25:00.000', 'level': 'rem', 'seconds': 870},
                     {'dateTime': '2020-09-05T06:39:30.000', 'level': 'light', 'seconds': 720},
                     {'dateTime': '2020-09-05T06:51:30.000', 'level': 'rem', 'seconds': 2670},
                     {'dateTime': '2020-09-05T07:36:00.000', 'level': 'light', 'seconds': 1140},
                     {'dateTime': '2020-09-05T07:55:00.000', 'level': 'rem', 'seconds': 540},
                     {'dateTime': '2020-09-05T08:04:00.000', 'level': 'wake', 'seconds': 690},
                     {'dateTime': '2020-09-05T08:15:30.000', 'level': 'light', 'seconds': 90},
                     {'dateTime': '2020-09-05T08:17:00.000', 'level': 'deep', 'seconds': 1800},
                     {'dateTime': '2020-09-05T08:47:00.000', 'level': 'light', 'seconds': 540},
                     {'dateTime': '2020-09-05T08:56:00.000', 'level': 'wake', 'seconds': 390},
                     {'dateTime': '2020-09-05T09:02:30.000', 'level': 'light', 'seconds': 360},
                     {'dateTime': '2020-09-05T09:08:30.000', 'level': 'wake', 'seconds': 210},
                     {'dateTime': '2020-09-05T09:12:00.000', 'level': 'light', 'seconds': 450},
                     {'dateTime': '2020-09-05T09:19:30.000', 'level': 'rem', 'seconds': 390},
                     {'dateTime': '2020-09-05T09:26:00.000', 'level': 'light', 'seconds': 240},
                     {'dateTime': '2020-09-05T09:30:00.000', 'level': 'wake', 'seconds': 360},
                     {'dateTime': '2020-09-05T09:36:00.000', 'level': 'light', 'seconds': 600},
                     {'dateTime': '2020-09-05T09:46:00.000', 'level': 'rem', 'seconds': 1680},
                     {'dateTime': '2020-09-05T10:14:00.000', 'level': 'light', 'seconds': 2640},
                     {'dateTime': '2020-09-05T10:58:00.000', 'level': 'rem', 'seconds': 360},
                     {'dateTime': '2020-09-05T11:04:00.000', 'level': 'light', 'seconds': 570},
                     {'dateTime': '2020-09-05T11:13:30.000', 'level': 'wake', 'seconds': 600},
                     {'dateTime': '2020-09-05T11:23:30.000', 'level': 'light', 'seconds': 660}],
            'shortData': [
                {'dateTime': '2020-09-05T02:45:00.000', 'level': 'wake', 'seconds': 120},
                {'dateTime': '2020-09-05T02:52:00.000', 'level': 'wake', 'seconds': 90},
                {'dateTime': '2020-09-05T03:27:30.000', 'level': 'wake', 'seconds': 120},
                {'dateTime': '2020-09-05T03:44:00.000', 'level': 'wake', 'seconds': 90},
                {'dateTime': '2020-09-05T06:03:30.000', 'level': 'wake', 'seconds': 90},
                {'dateTime': '2020-09-05T06:19:00.000', 'level': 'wake', 'seconds': 30},
                {'dateTime': '2020-09-05T06:24:30.000', 'level': 'wake', 'seconds': 30},
                {'dateTime': '2020-09-05T06:50:00.000', 'level': 'wake', 'seconds': 30},
                {'dateTime': '2020-09-05T06:54:00.000', 'level': 'wake', 'seconds': 30},
                {'dateTime': '2020-09-05T07:30:00.000', 'level': 'wake', 'seconds': 30},
                {'dateTime': '2020-09-05T07:54:30.000', 'level': 'wake', 'seconds': 30},
                {'dateTime': '2020-09-05T08:45:30.000', 'level': 'wake', 'seconds': 90},
                {'dateTime': '2020-09-05T10:14:00.000', 'level': 'wake', 'seconds': 150},
                {'dateTime': '2020-09-05T10:46:30.000', 'level': 'wake', 'seconds': 30},
                {'dateTime': '2020-09-05T11:11:00.000', 'level': 'wake', 'seconds': 60},
                {'dateTime': '2020-09-05T11:26:00.000', 'level': 'wake', 'seconds': 90},
                {'dateTime': '2020-09-05T11:29:30.000', 'level': 'wake', 'seconds': 90},
                {'dateTime': '2020-09-05T11:32:30.000', 'level': 'wake', 'seconds': 120}],
            'summary': {
                'deep': {'count': 5, 'minutes': 84, 'thirtyDayAvgMinutes': 70},
                'light': {'count': 30, 'minutes': 229, 'thirtyDayAvgMinutes': 252},
                'rem': {'count': 9, 'minutes': 143, 'thirtyDayAvgMinutes': 103},
                'wake': {'count': 27, 'minutes': 84, 'thirtyDayAvgMinutes': 69}}
        },
         'logId': 28751318002, 'minutesAfterWakeup': 0, 'minutesAsleep': 456, 'minutesAwake': 84,
         'minutesToFallAsleep': 0, 'startTime': '2020-09-05T02:34:30.000', 'timeInBed': 540, 'type': 'stages'}
    ], 'summary': {'stages': {'deep': 84, 'light': 229, 'rem': 143, 'wake': 84}, 'totalMinutesAsleep': 456,
                   'totalSleepRecords': 1, 'totalTimeInBed': 540}
}


#  I don't like how the above looks, but it's the best option vs. hundreds of lines just for test data.

@pytest.mark.skipif(os.getenv("TEST_LEVEL") != "local", reason='Travis-CI issues')
def test_query_sleep_data():
    database = Database('fitbit_test', 'sleep')
    connection = database.engine.connect()

    connection.execute(sleep_summary_table.delete())

    sleep_data = Sleep(config={
        'database': 'fitbit_test',
        'date': '2020-09-05'
    }).query_sleep_data()

    assert sleep_data == EXPECTED_API_RESPONSE


def test_parse_summary_response():
    parsed_response = parse_summary_response(EXPECTED_API_RESPONSE['sleep'][0])

    assert parsed_response.date == datetime.datetime(2020, 9, 5, 0, 0)
    assert parsed_response.deep == 84
    assert parsed_response.light == 229
    assert parsed_response.rem == 143
    assert parsed_response.wake == 84
    assert parsed_response.total_minutes_asleep == 456
    assert parsed_response.total_time_in_bed == 540
