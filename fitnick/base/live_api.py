import os
from datetime import datetime, timedelta

import requests


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


def _get_access_token(provider):
    if provider == 'google':
        return _get_google_access_token()
    if provider == 'fitbit':
        return _get_fitbit_access_token()
    raise HealthConfigurationError(
        f'Unsupported FITNICK_HEALTH_PROVIDER value "{provider}". Expected "google" or "fitbit".'
    )


def uses_live_health_api():
    if _is_offline_mode():
        return False

    provider = get_health_provider()
    return bool(_get_access_token(provider))


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


def get_daily_activity_summary(activity_date):
    provider = get_health_provider()
    if provider == 'google':
        return {'summary': {'steps': _google_daily_steps(activity_date)}}
    if provider == 'fitbit':
        return _provider_get(provider='fitbit', api_version='1', path=f'user/-/activities/date/{activity_date}.json')
    raise HealthConfigurationError(
        f'Unsupported FITNICK_HEALTH_PROVIDER value "{provider}". Expected "google" or "fitbit".'
    )


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

    latest = max(
        rows,
        key=lambda item: item.get('sleep', {}).get('interval', {}).get('endTime', ''),
    )
    sleep = latest.get('sleep', {})
    summary = sleep.get('summary', {})
    interval = sleep.get('interval', {})
    return {
        'date': interval.get('endTime', '')[:10],
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
            and os.getenv('FITBIT_CONSUMER_KEY')
            and os.getenv('FITBIT_AUTH_HEADER')
        )
    return False


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
