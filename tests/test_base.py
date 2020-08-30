from datetime import datetime

import fitbit

from fitnick.base.base import get_authorized_client, check_date


def test_get_authorized_client():
    assert type(get_authorized_client()) == fitbit.Fitbit


def test_check_date():
    test_date_xpass = '08-26-2020'
    assert check_date(test_date_xpass)
    test_date_xfail = datetime.today().strftime('%Y-%m-%d')
    assert not check_date(test_date_xfail)
