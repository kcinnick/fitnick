import argparse
import json
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
    response.raise_for_status()
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
    response.raise_for_status()
    return response.json()


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

    refresh_parser = subparsers.add_parser('refresh', help='Refresh access token from refresh token.')
    refresh_parser.add_argument('--client-id', required=True)
    refresh_parser.add_argument('--client-secret', required=True)
    refresh_parser.add_argument('--refresh-token', required=True)
    refresh_parser.add_argument('--print-env', action='store_true')

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


if __name__ == '__main__':
    main()
