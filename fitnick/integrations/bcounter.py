import os
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

from fitnick.base.live_api import get_latest_sleep_session


DEFAULT_TIMEZONE = 'America/New_York'
DEFAULT_SOURCE = 'google_health'


class BCounterConfigurationError(RuntimeError):
    pass


class BCounterAPIError(RuntimeError):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f'bCounter API request failed ({status_code}): {message}')


def _configuration(base_url=None, api_key=None, timezone_name=None):
    resolved_url = (base_url or os.getenv('BCOUNTER_BASE_URL') or '').strip().rstrip('/')
    resolved_key = api_key or os.getenv('BCOUNTER_API_KEY')
    resolved_timezone = timezone_name or os.getenv('BCOUNTER_TIMEZONE', DEFAULT_TIMEZONE)

    missing = []
    if not resolved_url:
        missing.append('BCOUNTER_BASE_URL')
    if not resolved_key:
        missing.append('BCOUNTER_API_KEY')
    if missing:
        raise BCounterConfigurationError(f'Missing required configuration: {", ".join(missing)}')

    try:
        timezone = ZoneInfo(resolved_timezone)
    except ZoneInfoNotFoundError as exc:
        raise BCounterConfigurationError(f'Unknown BCOUNTER_TIMEZONE value: {resolved_timezone}') from exc

    return resolved_url, resolved_key, timezone


def _parse_wake_time(value, timezone):
    if not value:
        raise BCounterConfigurationError('Latest sleep session did not include a wake_time timestamp.')
    normalized = value[:-1] + '+00:00' if value.endswith('Z') else value
    try:
        wake_time = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise BCounterConfigurationError(f'Invalid wake_time timestamp from health provider: {value}') from exc
    if wake_time.tzinfo is None:
        wake_time = wake_time.replace(tzinfo=timezone)
    return wake_time.astimezone(timezone)


def _response_error(response):
    try:
        payload = response.json()
    except ValueError:
        return response.text[:300] or 'Unknown response'
    if isinstance(payload, dict):
        return str(payload.get('detail') or payload.get('error') or payload)
    return str(payload)


def sync_latest_wake_time(
    base_url=None,
    api_key=None,
    timezone_name=None,
    source=DEFAULT_SOURCE,
    now=None,
    session=None,
):
    """Push today's latest sleep end time to bCounter if it is not already recorded."""
    resolved_url, resolved_key, timezone = _configuration(base_url, api_key, timezone_name)
    http = session or requests.Session()
    headers = {'X-API-Key': resolved_key, 'Accept': 'application/json'}

    sleep = get_latest_sleep_session()
    if not sleep:
        return {'ok': True, 'action': 'skipped', 'reason': 'no_sleep_session'}

    wake_time = _parse_wake_time(sleep.get('wake_time'), timezone)
    current_time = now or datetime.now(timezone)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone)
    else:
        current_time = current_time.astimezone(timezone)

    if wake_time.date() != current_time.date():
        return {
            'ok': True,
            'action': 'skipped',
            'reason': 'wake_time_not_today',
            'wake_time': wake_time.isoformat(),
        }

    response = http.get(f'{resolved_url}/wake-time', headers=headers, timeout=30)
    if not response.ok:
        raise BCounterAPIError(response.status_code, _response_error(response))

    existing = response.json().get('wake_time_iso')
    if existing:
        existing_wake_time = _parse_wake_time(existing, timezone)
        if existing_wake_time == wake_time:
            return {
                'ok': True,
                'action': 'skipped',
                'reason': 'already_synced',
                'wake_time': wake_time.isoformat(),
            }

    response = http.post(
        f'{resolved_url}/wake-time',
        headers={**headers, 'Content-Type': 'application/json'},
        json={'wake_time': wake_time.isoformat(), 'source': source},
        timeout=30,
    )
    if not response.ok:
        raise BCounterAPIError(response.status_code, _response_error(response))

    return {
        'ok': True,
        'action': 'pushed',
        'wake_time': wake_time.isoformat(),
        'bcounter': response.json(),
    }

