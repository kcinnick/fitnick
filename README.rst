in ms when available)

Rows with no available heart metrics are omitted.

Fitbit note: this endpoint currently requires ``FITNICK_HEALTH_PROVIDER=fitbit`` and valid Fitbit scopes for heart-rate/HRV access  * ``hrv_ms`` (Fitbit ``dailyRmssd`` or Google daily HRV milliseconds when available)
Provider note:

* ``FITNICK_HEALTH_PROVIDER=fitbit`` returns resting BPM and, when available, intraday avg/min/max plus HRV.
* ``FITNICK_HEALTH_PROVIDER=google`` returns daily resting BPM and daily HRV when those Google Health data types/scopes are available (``avg_bpm``/``min_bpm``/``max_bpm`` remain ``null`` in Google mode).

