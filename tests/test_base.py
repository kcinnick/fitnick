from datetime import datetime

import fitbit

from fitnick.base.base import get_authorized_client


def test_get_authorized_client():
    assert type(get_authorized_client()) == fitbit.Fitbit
