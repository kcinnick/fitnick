import os
from pathlib import Path

from fitnick.base.base import introspect_tokens


def _require_env(name):
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(f'{name} is not set. Add it to the Render service environment variables before running refresh_token.py.')


def _resolve_refresh_token():
    refresh_token = os.getenv('FITBIT_REFRESH_TOKEN')
    if refresh_token:
        return refresh_token

    token_path = Path('fitnick/base/fitbit_refresh_token.txt')
    if token_path.exists():
        return token_path.read_text().strip()

    raise RuntimeError(
        'FITBIT_REFRESH_TOKEN is not set and no local refresh token file was found. '
        'Set the Render env var or save a token in fitnick/base/fitbit_refresh_token.txt.'
    )


def _resolve_auth_header():
    return _require_env('FITBIT_AUTH_HEADER')


def _write_token_file(filename, value):
    path = Path('fitnick/base') / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)


def refresh_authorized_client():
    import requests

    refresh_token = _resolve_refresh_token()
    auth_header = _resolve_auth_header()

    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
    }
    response = requests.post(
        url='https://api.fitbit.com/oauth2/token',
        data=data,
        headers={
            'Authorization': f'Basic {auth_header}',
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    access_token = payload.get('access_token')
    new_refresh_token = payload.get('refresh_token') or refresh_token

    if access_token:
        os.environ['FITBIT_ACCESS_KEY'] = access_token
        os.environ['FITBIT_ACCESS_TOKEN'] = access_token
        _write_token_file('fitbit_access_key.txt', access_token)

    os.environ['FITBIT_REFRESH_TOKEN'] = new_refresh_token
    _write_token_file('fitbit_refresh_token.txt', new_refresh_token)

    print(payload)
    return payload


def auto_refresh():
    if os.getenv('FITBIT_CONSUMER_KEY') and os.getenv('FITBIT_AUTH_HEADER'):
        try:
            access_token_valid, refresh_token_valid = introspect_tokens()
        except KeyError:
            access_token_valid = False
            refresh_token_valid = bool(os.getenv('FITBIT_REFRESH_TOKEN'))
    else:
        access_token_valid = False
        refresh_token_valid = bool(os.getenv('FITBIT_REFRESH_TOKEN'))

    if not access_token_valid:
        if refresh_token_valid:
            refresh_authorized_client()
        else:
            print('No valid access token or refresh token available; set FITBIT_REFRESH_TOKEN and FITBIT_AUTH_HEADER.')
    else:
        print('access token valid')


def main():
    auto_refresh()


if __name__ == '__main__':
    main()
