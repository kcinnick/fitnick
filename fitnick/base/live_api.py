import os

import requests


class FitbitConfigurationError(RuntimeError):
    pass


class FitbitAPIError(RuntimeError):
    def __init__(self, status_code, error_type, message):
        self.status_code = status_code
        self.error_type = error_type
        self.message = message
        super().__init__(f'Fitbit API request failed ({status_code}, {error_type}): {message}')


def uses_live_fitbit_api():
    return os.getenv('FITNICK_OFFLINE_MODE') != '1' and bool(get_fitbit_access_token())


def get_fitbit_access_token():
    return os.getenv('FITBIT_ACCESS_TOKEN') or os.getenv('FITBIT_ACCESS_KEY')


def can_refresh_fitbit_token():
    return bool(
        os.getenv('FITBIT_REFRESH_TOKEN')
        and os.getenv('FITBIT_CONSUMER_KEY')
        and os.getenv('FITBIT_AUTH_HEADER')
    )


def fitbit_get(path, api_version='1', params=None):
    access_token = get_fitbit_access_token()
    if not access_token:
        raise FitbitConfigurationError(
            'Missing Fitbit access token. Set FITBIT_ACCESS_TOKEN or FITBIT_ACCESS_KEY before calling live Fitbit endpoints.'
        )

    response = requests.get(
        f'https://api.fitbit.com/{api_version}/{path.lstrip("/")}',
        headers={'Authorization': f'Bearer {access_token}'},
        params=params,
        timeout=30,
    )

    if response.ok:
        return response.json()

    try:
        payload = response.json()
    except ValueError:
        raise FitbitAPIError(
            status_code=response.status_code,
            error_type='invalid_response',
            message=response.text[:200],
        )

    error = payload.get('errors', [{}])[0]
    raise FitbitAPIError(
        status_code=response.status_code,
        error_type=error.get('errorType', 'unknown_error'),
        message=error.get('message', 'Unknown Fitbit API error'),
    )


def get_daily_activity_summary(activity_date):
    return fitbit_get(f'user/-/activities/date/{activity_date}.json')


def get_profile_summary():
    profile = fitbit_get('user/-/profile.json').get('user', {})
    return {
        'display_name': profile.get('displayName'),
        'member_since': profile.get('memberSince'),
        'age': profile.get('age'),
    }


def run_smoke_test():
    profile = get_profile_summary()
    return {
        'ok': True,
        'mode': 'live',
        'refresh_configured': can_refresh_fitbit_token(),
        'profile': profile,
    }
