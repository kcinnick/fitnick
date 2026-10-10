import argparse
import json
import os
import secrets
import urllib.parse

import requests


AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth'
TOKEN_URL = 'https://oauth2.googleapis.com/token'
DEFAULT_REDIRECT_URI = 'https://www.google.com'
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly',
    'https://www.googleapis.com/auth/googlehealth.sleep.readonly',
]


def _raise_for_google_oauth_error(response, action):
    try:
        payload = response.json()
        details = json.dumps(payload)
    except ValueError:
        details = response.text[:500]
    raise requests.HTTPError(
        f'Google OAuth {action} failed ({response.status_code}): {details}',
        response=response,
    )


def build_authorize_url(client_id, redirect_uri, scopes, state, prompt_consent):
    query = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'access_type': 'offline',
        'scope': ' '.join(scopes),
        'state': state,
        'include_granted_scopes': 'true',
    }
    if prompt_consent:
        query['prompt'] = 'consent'

    return f'{AUTH_URL}?{urllib.parse.urlencode(query)}'


def exchange_code(client_id, client_secret, redirect_uri, code):
    response = requests.post(
        TOKEN_URL,
        data={
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'grant_type': 'authorization_code',
            'code': code,
        },
        timeout=30,
    )
    if not response.ok:
        _raise_for_google_oauth_error(response, 'code exchange')
    return response.json()


def refresh_access_token(client_id, client_secret, refresh_token):
    response = requests.post(
        TOKEN_URL,
        data={
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token',
        },
        timeout=30,
    )
    if not response.ok:
        _raise_for_google_oauth_error(response, 'token refresh')
    return response.json()


def load_env_file(env_file):
    values = {}
    if not os.path.exists(env_file):
        return values

    with open(env_file, 'r') as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith('#') or '=' not in raw:
                continue
            key, value = raw.split('=', 1)
            key = key.strip()
            value = value.strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                value = value[1:-1]
            values[key] = value
    return values


def write_env_file(env_file, updates):
    lines = []
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            lines = f.readlines()

    seen = set()
    output = []
    for line in lines:
        if '=' not in line.strip() or line.strip().startswith('#'):
            output.append(line)
            continue
        key = line.split('=', 1)[0].strip()
        if key in updates:
            output.append(f'{key}={updates[key]}\n')
            seen.add(key)
        else:
            output.append(line)

    for key, value in updates.items():
        if key not in seen:
            output.append(f'{key}={value}\n')

    with open(env_file, 'w') as f:
        f.writelines(output)


def print_env_exports(payload, client_id, client_secret):
    access_token = payload.get('access_token')
    refresh_token = payload.get('refresh_token')

    print('\nSet these in your local shell (PowerShell):')
    if access_token:
        print(f'$env:GOOGLE_HEALTH_ACCESS_TOKEN = "{access_token}"')
        print(f'$env:HEALTH_ACCESS_TOKEN = "{access_token}"')
    if refresh_token:
        print(f'$env:GOOGLE_HEALTH_REFRESH_TOKEN = "{refresh_token}"')
    print(f'$env:GOOGLE_HEALTH_CLIENT_ID = "{client_id}"')
    print(f'$env:GOOGLE_HEALTH_CLIENT_SECRET = "{client_secret}"')
    print('$env:FITNICK_HEALTH_PROVIDER = "google"')
    print('$env:FITNICK_OFFLINE_MODE = "0"')

    print('\nSet these in Render environment variables:')
    if access_token:
        print('GOOGLE_HEALTH_ACCESS_TOKEN=<access_token>')
    if refresh_token:
        print('GOOGLE_HEALTH_REFRESH_TOKEN=<refresh_token>')
    print('GOOGLE_HEALTH_CLIENT_ID=<client_id>')
    print('GOOGLE_HEALTH_CLIENT_SECRET=<client_secret>')
    print('FITNICK_HEALTH_PROVIDER=google')
    print('FITNICK_OFFLINE_MODE=0')
    print('\nPaste this into your .env file:')
    if access_token:
        print(f'GOOGLE_HEALTH_ACCESS_TOKEN={access_token}')
        print(f'HEALTH_ACCESS_TOKEN={access_token}')
    if refresh_token:
        print(f'GOOGLE_HEALTH_REFRESH_TOKEN={refresh_token}')
    print(f'GOOGLE_HEALTH_CLIENT_ID={client_id}')
    print(f'GOOGLE_HEALTH_CLIENT_SECRET={client_secret}')
    print('FITNICK_HEALTH_PROVIDER=google')
    print('FITNICK_OFFLINE_MODE=0')


def build_parser():
    parser = argparse.ArgumentParser(description='Google Health OAuth helper for FitNick.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    auth_parser = subparsers.add_parser('auth-url', help='Generate OAuth consent URL.')
    auth_parser.add_argument('--client-id', required=True)
    auth_parser.add_argument('--redirect-uri', default=DEFAULT_REDIRECT_URI)
    auth_parser.add_argument('--scope', action='append', default=[])
    auth_parser.add_argument('--prompt-consent', action='store_true')
    auth_parser.add_argument('--state', default='')

    exchange_parser = subparsers.add_parser('exchange-code', help='Exchange OAuth code for tokens.')
    exchange_parser.add_argument('--client-id', required=True)
    exchange_parser.add_argument('--client-secret', required=True)
    exchange_parser.add_argument('--code', required=True)
    exchange_parser.add_argument('--redirect-uri', default=DEFAULT_REDIRECT_URI)
    exchange_parser.add_argument('--print-env', action='store_true')
    exchange_parser.add_argument('--write-env-file', default='')

    refresh_parser = subparsers.add_parser('refresh', help='Refresh access token from refresh token.')
    refresh_parser.add_argument('--client-id', required=True)
    refresh_parser.add_argument('--client-secret', required=True)
    refresh_parser.add_argument('--refresh-token', required=True)
    refresh_parser.add_argument('--print-env', action='store_true')
    refresh_parser.add_argument('--write-env-file', default='')

    refresh_env_parser = subparsers.add_parser('refresh-from-env', help='Refresh access token using values from .env.')
    refresh_env_parser.add_argument('--env-file', default='.env')

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == 'auth-url':
        scopes = args.scope or DEFAULT_SCOPES
        state = args.state or secrets.token_urlsafe(16)
        url = build_authorize_url(
            client_id=args.client_id,
            redirect_uri=args.redirect_uri,
            scopes=scopes,
            state=state,
            prompt_consent=args.prompt_consent,
        )
        print(url)
        return

    if args.command == 'exchange-code':
        payload = exchange_code(
            client_id=args.client_id,
            client_secret=args.client_secret,
            redirect_uri=args.redirect_uri,
            code=args.code,
        )
        print(json.dumps(payload, indent=2))
        if args.print_env:
            print_env_exports(payload, args.client_id, args.client_secret)
        if args.write_env_file:
            updates = {
                'GOOGLE_HEALTH_CLIENT_ID': args.client_id,
                'GOOGLE_HEALTH_CLIENT_SECRET': args.client_secret,
                'FITNICK_HEALTH_PROVIDER': 'google',
                'FITNICK_OFFLINE_MODE': '0',
            }
            if payload.get('access_token'):
                updates['GOOGLE_HEALTH_ACCESS_TOKEN'] = payload['access_token']
                updates['HEALTH_ACCESS_TOKEN'] = payload['access_token']
            if payload.get('refresh_token'):
                updates['GOOGLE_HEALTH_REFRESH_TOKEN'] = payload['refresh_token']
            write_env_file(args.write_env_file, updates)
            print(f'Updated {args.write_env_file}')
        return

    if args.command == 'refresh':
        payload = refresh_access_token(
            client_id=args.client_id,
            client_secret=args.client_secret,
            refresh_token=args.refresh_token,
        )
        print(json.dumps(payload, indent=2))
        if args.print_env:
            print_env_exports(payload, args.client_id, args.client_secret)
        if args.write_env_file:
            updates = {}
            if payload.get('access_token'):
                updates['GOOGLE_HEALTH_ACCESS_TOKEN'] = payload['access_token']
                updates['HEALTH_ACCESS_TOKEN'] = payload['access_token']
            if payload.get('refresh_token'):
                updates['GOOGLE_HEALTH_REFRESH_TOKEN'] = payload['refresh_token']
            if updates:
                write_env_file(args.write_env_file, updates)
                print(f'Updated {args.write_env_file}')
        return

    if args.command == 'refresh-from-env':
        env_values = load_env_file(args.env_file)
        client_id = env_values.get('GOOGLE_HEALTH_CLIENT_ID') or os.getenv('GOOGLE_HEALTH_CLIENT_ID')
        client_secret = env_values.get('GOOGLE_HEALTH_CLIENT_SECRET') or os.getenv('GOOGLE_HEALTH_CLIENT_SECRET')
        refresh_token = env_values.get('GOOGLE_HEALTH_REFRESH_TOKEN') or os.getenv('GOOGLE_HEALTH_REFRESH_TOKEN')

        missing = []
        if not client_id:
            missing.append('GOOGLE_HEALTH_CLIENT_ID')
        if not client_secret:
            missing.append('GOOGLE_HEALTH_CLIENT_SECRET')
        if not refresh_token:
            missing.append('GOOGLE_HEALTH_REFRESH_TOKEN')
        if missing:
            raise SystemExit(f'Missing values for {", ".join(missing)} in {args.env_file} or environment.')

        payload = refresh_access_token(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )
        print(json.dumps(payload, indent=2))

        updates = {}
        if payload.get('access_token'):
            updates['GOOGLE_HEALTH_ACCESS_TOKEN'] = payload['access_token']
            updates['HEALTH_ACCESS_TOKEN'] = payload['access_token']
        if payload.get('refresh_token'):
            updates['GOOGLE_HEALTH_REFRESH_TOKEN'] = payload['refresh_token']
        if updates:
            write_env_file(args.env_file, updates)
            print(f'Updated {args.env_file}')


if __name__ == '__main__':
    main()
