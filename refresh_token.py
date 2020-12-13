import os
from fitnick.base.base import introspect_tokens


def refresh_authorized_client():
    import requests
    with requests.session() as session:
        with open('fitnick/base/fitbit_refresh_token.txt', 'r') as f:
            refresh_token = f.read().strip()
            with open('fitnick/base/fitbit_refresh_token_old.txt', 'w') as f:
                #  write the old token to file, just in case..
                f.write(refresh_token)
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'expires_in': 31536000  # 12 hours
            }
        r = session.post(
            url='https://api.fitbit.com/oauth2/token',
            data=data,
            headers={
                'clientId': '22BWR3',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f"Basic {os.environ['FITBIT_AUTH_HEADER']}"}
        )
        try:
            os.environ['FITBIT_ACCESS_KEY'] = r.json()['access_token']
            os.environ['FITBIT_REFRESH_TOKEN'] = r.json()['refresh_token']
            with open('fitnick/base/fitbit_access_key.txt', 'w') as f:
                f.write(r.json()['access_token'])
            with open('fitnick/base/fitbit_refresh_token.txt', 'w') as f:
                f.write(r.json()['refresh_token'])
        except KeyError:
            print('ERROR: {}', r.json())
        print(r.json())
    return


def auto_refresh():
    access_token_valid, refresh_token_valid = introspect_tokens()
    if not access_token_valid:
        if refresh_token_valid:
            refresh_authorized_client()
    else:
        print('access token valid')


def main():
    auto_refresh()


if __name__ == '__main__':
    main()
