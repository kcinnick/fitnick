import os
import time
from datetime import datetime, timedelta

import requests


GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token'
FITBIT_TOKEN_URL = 'https://api.fitbit.com/oauth2/token'
MAX_DAILY_HEART_RANGE_DAYS = 90
_PROACTIVE_REFRESH_LAST_ATTEMPT = {}


class HealthConfigurationError(RuntimeError):
    pass


class HealthAPIError(RuntimeError):
    def __init__(self, status_code, provider, error_type, message):
        self.status_code = status_code
        self.provider = provider
        self.error_type = error_type
        self.message = message
        super().__init__(f'{provider} API request failed ({status_code}, {error_type}): {message}')


def get_health_provider():
    return os.getenv('FITNICK_HEALTH_PROVIDER', 'google').strip().lower()


def _is_offline_mode():
    return os.getenv('FITNICK_OFFLINE_MODE') == '1'


def _get_google_access_token():
    return os.getenv('GOOGLE_HEALTH_ACCESS_TOKEN') or os.getenv('HEALTH_ACCESS_TOKEN')


def _get_fitbit_access_token():
    return os.getenv('FITBIT_ACCESS_TOKEN') or os.getenv('FITBIT_ACCESS_KEY')


def _get_token_expiry_env_key(provider):
    if provider == 'google':
        return 'GOOGLE_HEALTH_ACCESS_TOKEN_EXPIRES_AT'
    if provider == 'fitbit':
        return 'FITBIT_ACCESS_TOKEN_EXPIRES_AT'
    return None


def _set_token_expiry(provider, payload):
    env_key = _get_token_expiry_env_key(provider)
    if not env_key:
        return

    expires_in = payload.get('expires_in')
    if expires_in is None:
        return

    try:
        expires_in_seconds = int(float(expires_in))
    except (TypeError, ValueError):
        return

    # Keep a small margin for clock drift.
    os.environ[env_key] = str(int(time.time()) + max(0, expires_in_seconds) - 5)


def _get_token_expiry(provider):
    env_key = _get_token_expiry_env_key(provider)
    if not env_key:
        return None

    raw_value = os.getenv(env_key)
    if not raw_value:
        return None

    try:
        return int(raw_value)
    except ValueError:
        return None


def _ensure_fresh_access_token(provider):
    if os.getenv('FITNICK_AUTO_REFRESH_TOKENS', '1') != '1':
        return
    if os.getenv('FITNICK_PROACTIVE_REFRESH_TOKENS', '1') != '1':
        return
    if not can_refresh_health_token():
        return

    now = int(time.time())
    refresh_buffer = int(os.getenv('FITNICK_TOKEN_REFRESH_BUFFER_SECONDS', '300'))
    unknown_interval = int(os.getenv('FITNICK_TOKEN_REFRESH_UNKNOWN_INTERVAL_SECONDS', '3600'))

    token_expiry = _get_token_expiry(provider)
    if token_expiry is None:
        last_attempt = _PROACTIVE_REFRESH_LAST_ATTEMPT.get(provider, 0)
        if now - last_attempt < max(60, unknown_interval):
            return
    elif now < token_expiry - max(0, refresh_buffer):
        return

    _PROACTIVE_REFRESH_LAST_ATTEMPT[provider] = now
    _refresh_access_token(provider)


def _get_access_token(provider):
    access_token = None
    if provider == 'google':
        access_token = _get_google_access_token()
    elif provider == 'fitbit':
        access_token = _get_fitbit_access_token()
    else:
        raise HealthConfigurationError(
            f'Unsupported FITNICK_HEALTH_PROVIDER value "{provider}". Expected "google" or "fitbit".'
        )

    if access_token:
        return access_token

    # If access token is missing but refresh credentials exist, attempt one refresh.
    refreshed = _refresh_access_token(provider)
    if refreshed:
        return refreshed

    raise HealthConfigurationError(
        f'Missing access token for provider "{provider}". Configure env vars before calling live endpoints.'
    )


def uses_live_health_api():
    if _is_offline_mode():
        return False

    provider = get_health_provider()
    try:
        return bool(_get_access_token(provider))
    except HealthConfigurationError:
        return False


def _parse_error_payload(response):
    try:
        payload = response.json()
    except ValueError:
        return 'invalid_response', response.text[:200]

    if isinstance(payload, dict):
        if isinstance(payload.get('error'), dict):
            err = payload['error']
            return str(err.get('status', 'unknown_error')).lower(), err.get('message', 'Unknown API error')

        errors = payload.get('errors')
        if isinstance(errors, list) and errors:
            err = errors[0]
            return err.get('errorType', 'unknown_error'), err.get('message', 'Unknown API error')

    return 'unknown_error', 'Unknown API error'


def _provider_get(provider, path, api_version, params=None):
    _ensure_fresh_access_token(provider)
    access_token = _get_access_token(provider)
    if not access_token:
        raise HealthConfigurationError(
            f'Missing access token for provider "{provider}". Configure env vars before calling live endpoints.'
        )

    if provider == 'google':
        base_url = f'https://health.googleapis.com/{api_version}/{path.lstrip("/")}'
    else:
        base_url = f'https://api.fitbit.com/{api_version}/{path.lstrip("/")}'

    response = requests.get(
        base_url,
        headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'},
        params=params,
        timeout=30,
    )

    if response.status_code == 401 and can_refresh_health_token():
        refreshed = _refresh_access_token(provider)
        if refreshed:
            response = requests.get(
                base_url,
                headers={'Authorization': f'Bearer {refreshed}', 'Accept': 'application/json'},
                params=params,
                timeout=30,
            )

    if response.ok:
        return response.json()

    error_type, message = _parse_error_payload(response)
    raise HealthAPIError(
        status_code=response.status_code,
        provider=provider,
        error_type=error_type,
        message=message,
    )


def _provider_post(provider, path, api_version, payload):
    _ensure_fresh_access_token(provider)
    access_token = _get_access_token(provider)
    if not access_token:
        raise HealthConfigurationError(
            f'Missing access token for provider "{provider}". Configure env vars before calling live endpoints.'
        )

    if provider == 'google':
        base_url = f'https://health.googleapis.com/{api_version}/{path.lstrip("/")}'
    else:
        base_url = f'https://api.fitbit.com/{api_version}/{path.lstrip("/")}'

    response = requests.post(
        base_url,
        headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'},
        json=payload,
        timeout=30,
    )

    if response.status_code == 401 and can_refresh_health_token():
        refreshed = _refresh_access_token(provider)
        if refreshed:
            response = requests.post(
                base_url,
                headers={'Authorization': f'Bearer {refreshed}', 'Accept': 'application/json'},
                json=payload,
                timeout=30,
            )

    if response.ok:
        return response.json()

    error_type, message = _parse_error_payload(response)
    raise HealthAPIError(
        status_code=response.status_code,
        provider=provider,
        error_type=error_type,
        message=message,
    )


def _google_daily_steps(activity_date):
    requested_date = datetime.strptime(activity_date, '%Y-%m-%d').date()
    end_date = requested_date + timedelta(days=1)
    request_payload = {
        'range': {
            'start': {
                'date': {'year': requested_date.year, 'month': requested_date.month, 'day': requested_date.day},
                'time': {'hours': 0, 'minutes': 0, 'seconds': 0, 'nanos': 0},
            },
            'end': {
                'date': {'year': end_date.year, 'month': end_date.month, 'day': end_date.day},
                'time': {'hours': 0, 'minutes': 0, 'seconds': 0, 'nanos': 0},
            },
        },
        'windowSizeDays': 1,
        'dataSourceFamily': 'users/me/dataSourceFamilies/google-sources',
    }
    payload = _provider_post(
        provider='google',
        api_version='v4',
        path='users/me/dataTypes/steps/dataPoints:dailyRollUp',
        payload=request_payload,
    )
    rollups = payload.get('rollupDataPoints', [])
    if not rollups:
        return 0
    return int(rollups[0].get('steps', {}).get('countSum', 0))


def _google_daily_steps_rollup(start_date, end_date):
    request_payload = {
        'range': {
            'start': {
                'date': {'year': start_date.year, 'month': start_date.month, 'day': start_date.day},
                'time': {'hours': 0, 'minutes': 0, 'seconds': 0, 'nanos': 0},
            },
            'end': {
                'date': {'year': end_date.year, 'month': end_date.month, 'day': end_date.day},
                'time': {'hours': 0, 'minutes': 0, 'seconds': 0, 'nanos': 0},
            },
        },
        'windowSizeDays': 1,
        'dataSourceFamily': 'users/me/dataSourceFamilies/google-sources',
    }
    return _provider_post(
        provider='google',
        api_version='v4',
        path='users/me/dataTypes/steps/dataPoints:dailyRollUp',
        payload=request_payload,
    )


def get_recent_steps(days=7):
    provider = get_health_provider()
    if days < 1:
        return []

    if provider == 'google':
        today = datetime.utcnow().date()
        start_date = today - timedelta(days=days - 1)
        end_date = today + timedelta(days=1)
        payload = _google_daily_steps_rollup(start_date=start_date, end_date=end_date)
        rows = []
        for row in payload.get('rollupDataPoints', []):
            civil_start = row.get('civilStartTime', {}).get('date', {})
            if not civil_start:
                continue
            year = civil_start.get('year')
            month = civil_start.get('month')
            day = civil_start.get('day')
            if year is None or month is None or day is None:
                continue
            rows.append({
                'date': f'{year:04d}-{month:02d}-{day:02d}',
                'steps': int(row.get('steps', {}).get('countSum', 0)),
            })
        rows.sort(key=lambda item: item['date'])
        return rows

    if provider == 'fitbit':
        today = datetime.utcnow().date()
        rows = []
        for offset in range(days - 1, -1, -1):
            target = (today - timedelta(days=offset)).strftime('%Y-%m-%d')
            payload = _provider_get(provider='fitbit', api_version='1', path=f'user/-/activities/date/{target}.json')
            rows.append({'date': target, 'steps': int(payload.get('summary', {}).get('steps', 0))})
        return rows

    raise HealthConfigurationError(
        f'Unsupported FITNICK_HEALTH_PROVIDER value "{provider}". Expected "google" or "fitbit".'
    )


def _date_range_inclusive(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _coerce_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_daily_rmssd(hrv_payload, target_date_str):
    rows = hrv_payload.get('hrv', []) if isinstance(hrv_payload, dict) else []
    for row in rows:
        if row.get('dateTime') != target_date_str:
            continue
        value = row.get('value', {})
        if not isinstance(value, dict):
            return None
        return _coerce_float(value.get('dailyRmssd'))
    return None


def _coerce_iso_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        return None


def _google_date_to_iso(date_payload):
    if not isinstance(date_payload, dict):
        return None
    year = date_payload.get('year')
    month = date_payload.get('month')
    day = date_payload.get('day')
    if year is None or month is None or day is None:
        return None
    try:
        return f'{int(year):04d}-{int(month):02d}-{int(day):02d}'
    except (TypeError, ValueError):
        return None


def _google_daily_data_points(data_type, filter_param, start_date, end_date):
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    date_filter = f'{filter_param}.date >= "{start_str}" and {filter_param}.date <= "{end_str}"'
    params = {
        'filter': date_filter,
    }
    try:
        payload = _provider_get(
            provider='google',
            api_version='v4',
            path=f'users/me/dataTypes/{data_type}/dataPoints',
            params=params,
        )
    except HealthAPIError as exc:
        if exc.status_code != 400:
            raise
        payload = _provider_get(
            provider='google',
            api_version='v4',
            path=f'users/me/dataTypes/{data_type}/dataPoints',
        )
    return payload.get('dataPoints', []) if isinstance(payload, dict) else []


def _google_daily_heart_rate_metrics(start_date, end_date):
    rows_by_date = {}

    resting_rows = _google_daily_data_points(
        data_type='daily-resting-heart-rate',
        filter_param='daily_resting_heart_rate',
        start_date=start_date,
        end_date=end_date,
    )
    for row in resting_rows:
        daily_resting = row.get('dailyRestingHeartRate', {}) if isinstance(row, dict) else {}
        on_date = _google_date_to_iso(daily_resting.get('date'))
        if not on_date:
            continue
        parsed = _coerce_iso_date(on_date)
        if parsed is None or parsed < start_date or parsed > end_date:
            continue
        entry = rows_by_date.setdefault(
            on_date,
            {
                'on_date': on_date,
                'resting_bpm': None,
                'avg_bpm': None,
                'min_bpm': None,
                'max_bpm': None,
                'hrv_ms': None,
            },
        )
        entry['resting_bpm'] = _coerce_int(daily_resting.get('beatsPerMinute'))

    hrv_rows = _google_daily_data_points(
        data_type='daily-heart-rate-variability',
        filter_param='daily_heart_rate_variability',
        start_date=start_date,
        end_date=end_date,
    )
    for row in hrv_rows:
        daily_hrv = row.get('dailyHeartRateVariability', {}) if isinstance(row, dict) else {}
        on_date = _google_date_to_iso(daily_hrv.get('date'))
        if not on_date:
            continue
        parsed = _coerce_iso_date(on_date)
        if parsed is None or parsed < start_date or parsed > end_date:
            continue
        entry = rows_by_date.setdefault(
            on_date,
            {
                'on_date': on_date,
                'resting_bpm': None,
                'avg_bpm': None,
                'min_bpm': None,
                'max_bpm': None,
                'hrv_ms': None,
            },
        )
        entry['hrv_ms'] = _coerce_float(daily_hrv.get('averageHeartRateVariabilityMilliseconds'))

    rows = list(rows_by_date.values())
    rows.sort(key=lambda item: item['on_date'])
    return rows


def _fitbit_daily_heart_rate_metrics(start_date, end_date):
    resting_payload = _provider_get(
        provider='fitbit',
        api_version='1',
        path=f'user/-/activities/heart/date/{start_date}/{end_date}.json',
    )
    resting_rows = resting_payload.get('activities-heart', []) if isinstance(resting_payload, dict) else []
    resting_by_date = {}
    for row in resting_rows:
        on_date = row.get('dateTime')
        if not on_date:
            continue
        resting = row.get('value', {}).get('restingHeartRate') if isinstance(row.get('value', {}), dict) else None
        resting_by_date[on_date] = _coerce_int(resting)

    results = []
    for day in _date_range_inclusive(start_date, end_date):
        day_str = day.strftime('%Y-%m-%d')
        intraday = None
        hrv_payload = None

        try:
            intraday = _provider_get(
                provider='fitbit',
                api_version='1',
                path=f'user/-/activities/heart/date/{day_str}/1d/1min.json',
            )
        except HealthAPIError as exc:
            if exc.status_code not in {403, 404}:
                raise

        try:
            hrv_payload = _provider_get(
                provider='fitbit',
                api_version='1',
                path=f'user/-/hrv/date/{day_str}.json',
            )
        except HealthAPIError as exc:
            if exc.status_code not in {403, 404}:
                raise

        dataset = []
        if isinstance(intraday, dict):
            dataset = intraday.get('activities-heart-intraday', {}).get('dataset', [])

        values = []
        for item in dataset:
            bpm = _coerce_int(item.get('value')) if isinstance(item, dict) else None
            if bpm is not None:
                values.append(bpm)

        avg_bpm = int(round(sum(values) / len(values))) if values else None
        min_bpm = min(values) if values else None
        max_bpm = max(values) if values else None
        hrv_ms = _extract_daily_rmssd(hrv_payload, day_str) if hrv_payload else None
        resting_bpm = resting_by_date.get(day_str)

        if all(metric is None for metric in (resting_bpm, avg_bpm, min_bpm, max_bpm, hrv_ms)):
            continue

        results.append(
            {
                'on_date': day_str,
                'resting_bpm': resting_bpm,
                'avg_bpm': avg_bpm,
                'min_bpm': min_bpm,
                'max_bpm': max_bpm,
                'hrv_ms': hrv_ms,
            }
        )

    results.sort(key=lambda item: item['on_date'])
    return results


def get_daily_heart_rate_metrics(start_date, end_date):
    provider = get_health_provider()
    if provider == 'fitbit':
        return _fitbit_daily_heart_rate_metrics(start_date=start_date, end_date=end_date)
    if provider == 'google':
        return _google_daily_heart_rate_metrics(start_date=start_date, end_date=end_date)
    raise HealthConfigurationError(
        f'Unsupported FITNICK_HEALTH_PROVIDER value "{provider}". Expected "google" or "fitbit".'
    )


def get_daily_activity_summary(activity_date):
    provider = get_health_provider()
    if provider == 'google':
        return {'summary': {'steps': _google_daily_steps(activity_date)}}
    if provider == 'fitbit':
        return _provider_get(provider='fitbit', api_version='1', path=f'user/-/activities/date/{activity_date}.json')
    raise HealthConfigurationError(
        f'Unsupported FITNICK_HEALTH_PROVIDER value "{provider}". Expected "google" or "fitbit".'
    )


def _parse_iso_datetime(value):
    if not value:
        return None
    normalized = value[:-1] + '+00:00' if value.endswith('Z') else value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _sleep_duration_minutes(row):
    sleep = row.get('sleep', {})
    summary = sleep.get('summary', {})
    try:
        summary_minutes = int(summary.get('minutesAsleep') or 0) + int(summary.get('minutesAwake') or 0)
    except (TypeError, ValueError):
        summary_minutes = 0
    if summary_minutes > 0:
        return summary_minutes

    interval = sleep.get('interval', {})
    start_time = _parse_iso_datetime(interval.get('startTime', ''))
    end_time = _parse_iso_datetime(interval.get('endTime', ''))
    if start_time is None or end_time is None:
        return 0
    try:
        return max(0, int((end_time - start_time).total_seconds() // 60))
    except TypeError:
        return 0


def _select_latest_overnight_sleep(rows):
    try:
        minimum_minutes = max(1, int(os.getenv('FITNICK_OVERNIGHT_SLEEP_MINUTES', '180')))
    except ValueError:
        minimum_minutes = 180
    candidates = []
    for row in rows:
        interval = row.get('sleep', {}).get('interval', {})
        end_time = _parse_iso_datetime(interval.get('endTime', ''))
        if end_time is None:
            continue
        start_time = _parse_iso_datetime(interval.get('startTime', ''))
        duration_minutes = _sleep_duration_minutes(row)
        crosses_midnight = start_time is not None and start_time.date() < end_time.date()
        candidates.append((row, end_time, duration_minutes, crosses_midnight))

    if not candidates:
        return None

    overnight = [
        candidate for candidate in candidates
        if candidate[3] or candidate[2] >= minimum_minutes
    ]
    selection_pool = overnight or candidates
    latest_wake_date = max(candidate[1].date() for candidate in selection_pool)
    same_date = [candidate for candidate in selection_pool if candidate[1].date() == latest_wake_date]
    return max(same_date, key=lambda candidate: (candidate[2], candidate[1]))[0]


def get_latest_sleep_session(lookback_days=14):
    provider = get_health_provider()
    if provider != 'google':
        return None

    cutoff = (datetime.utcnow().date() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    payload = _provider_get(
        provider='google',
        api_version='v4',
        path='users/me/dataTypes/sleep/dataPoints:reconcile',
        params={
            'dataSourceFamily': 'users/me/dataSourceFamilies/google-sources',
            'filter': f'sleep.interval.civil_end_time >= "{cutoff}"',
        },
    )

    rows = payload.get('dataPoints', [])
    if not rows:
        return None

    latest = _select_latest_overnight_sleep(rows)
    if latest is None:
        return None
    sleep = latest.get('sleep', {})
    summary = sleep.get('summary', {})
    interval = sleep.get('interval', {})
    wake_time = interval.get('endTime', '')
    return {
        'date': wake_time[:10],
        'wake_time': wake_time,
        'minutes_asleep': int(summary.get('minutesAsleep', 0)),
        'minutes_awake': int(summary.get('minutesAwake', 0)),
    }


def get_latest_body_fat_entry(lookback_days=120):
    provider = get_health_provider()
    if provider != 'google':
        return None

    cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).strftime('%Y-%m-%dT00:00:00Z')
    payload = _provider_get(
        provider='google',
        api_version='v4',
        path='users/me/dataTypes/body-fat/dataPoints',
        params={'filter': f'body_fat.sample_time.physical_time >= "{cutoff}"'},
    )

    rows = payload.get('dataPoints', [])
    if not rows:
        return None

    latest = max(
        rows,
        key=lambda item: item.get('bodyFat', {}).get('sampleTime', {}).get('physicalTime', ''),
    )
    body_fat = latest.get('bodyFat', {})
    sample_time = body_fat.get('sampleTime', {}).get('physicalTime', '')
    return {
        'date': sample_time[:10],
        'percentage': body_fat.get('percentage'),
    }


def get_body_weight(start_date, end_date):
    """Return Google Health weight samples in the requested inclusive date range."""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    rows = _google_daily_data_points(
        data_type='weight',
        filter_param='weight',
        start_date=start_date,
        end_date=end_date,
    )
    result = []
    for row in rows:
        weight = row.get('weight', {}) if isinstance(row, dict) else {}
        sample_time = weight.get('sampleTime', {}).get('physicalTime', '')
        if not sample_time:
            continue
        on_date = sample_time[:10]
        parsed_date = _coerce_iso_date(on_date)
        if parsed_date is None or parsed_date < start_date or parsed_date > end_date:
            continue
        kilograms = _coerce_float(weight.get('kilograms'))
        if kilograms is not None:
            result.append({'date': on_date, 'kilograms': kilograms})
    return result


def get_identity_summary():
    provider = get_health_provider()
    if provider == 'google':
        identity = _provider_get(provider='google', api_version='v4', path='users/me/identity')
        return {
            'provider': 'google',
            'health_user_id': identity.get('healthUserId'),
            'legacy_user_id': identity.get('legacyUserId'),
        }
    if provider == 'fitbit':
        profile = _provider_get(provider='fitbit', api_version='1', path='user/-/profile.json').get('user', {})
        return {
            'provider': 'fitbit',
            'display_name': profile.get('displayName'),
            'member_since': profile.get('memberSince'),
            'age': profile.get('age'),
        }
    raise HealthConfigurationError(
        f'Unsupported FITNICK_HEALTH_PROVIDER value "{provider}". Expected "google" or "fitbit".'
    )


def can_refresh_health_token():
    provider = get_health_provider()
    if provider == 'google':
        return bool(
            os.getenv('GOOGLE_HEALTH_REFRESH_TOKEN')
            and os.getenv('GOOGLE_HEALTH_CLIENT_ID')
            and os.getenv('GOOGLE_HEALTH_CLIENT_SECRET')
        )
    if provider == 'fitbit':
        return bool(
            os.getenv('FITBIT_REFRESH_TOKEN')
            and os.getenv('FITBIT_AUTH_HEADER')
        )
    return False


def _refresh_google_access_token():
    refresh_token = os.getenv('GOOGLE_HEALTH_REFRESH_TOKEN')
    client_id = os.getenv('GOOGLE_HEALTH_CLIENT_ID')
    client_secret = os.getenv('GOOGLE_HEALTH_CLIENT_SECRET')
    if not refresh_token or not client_id or not client_secret:
        return None

    response = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token',
        },
        timeout=30,
    )
    if not response.ok:
        return None

    payload = response.json()
    _set_token_expiry('google', payload)
    access_token = payload.get('access_token')
    if not access_token:
        return None

    os.environ['GOOGLE_HEALTH_ACCESS_TOKEN'] = access_token
    os.environ['HEALTH_ACCESS_TOKEN'] = access_token
    if payload.get('refresh_token'):
        os.environ['GOOGLE_HEALTH_REFRESH_TOKEN'] = payload['refresh_token']
    return access_token


def _refresh_fitbit_access_token():
    refresh_token = os.getenv('FITBIT_REFRESH_TOKEN')
    auth_header = os.getenv('FITBIT_AUTH_HEADER')
    if not refresh_token or not auth_header:
        return None

    response = requests.post(
        FITBIT_TOKEN_URL,
        data={
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
        },
        headers={
            'Authorization': f'Basic {auth_header}',
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        timeout=30,
    )
    if not response.ok:
        return None

    payload = response.json()
    _set_token_expiry('fitbit', payload)
    access_token = payload.get('access_token')
    if not access_token:
        return None

    os.environ['FITBIT_ACCESS_TOKEN'] = access_token
    os.environ['FITBIT_ACCESS_KEY'] = access_token
    if payload.get('refresh_token'):
        os.environ['FITBIT_REFRESH_TOKEN'] = payload['refresh_token']
    return access_token


def _refresh_access_token(provider):
    if os.getenv('FITNICK_AUTO_REFRESH_TOKENS', '1') != '1':
        return None
    if provider == 'google':
        return _refresh_google_access_token()
    if provider == 'fitbit':
        return _refresh_fitbit_access_token()
    return None


def run_smoke_test():
    if _is_offline_mode():
        return {
            'ok': False,
            'provider': get_health_provider(),
            'mode': 'offline',
            'refresh_configured': False,
            'error': 'Offline mode is enabled (FITNICK_OFFLINE_MODE=1).',
        }

    identity = get_identity_summary()
    return {
        'ok': True,
        'provider': get_health_provider(),
        'mode': 'live',
        'refresh_configured': can_refresh_health_token(),
        'identity': identity,
    }


# Backwards-compatible aliases for recently added Fitbit-specific view code.
FitbitConfigurationError = HealthConfigurationError
FitbitAPIError = HealthAPIError


def uses_live_fitbit_api():
    return uses_live_health_api()
