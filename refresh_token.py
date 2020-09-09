import requests
from bs4 import BeautifulSoup
import os


def refresh_authorized_client():
    import requests
    with requests.session() as session:
        data = {'grant_type': 'refresh_token',
                'refresh_token': os.environ['FITBIT_REFRESH_TOKEN']}
        r = session.post(
            url='https://api.fitbit.com/oauth2/token',
            data=data,
            headers={
                'clientId': '22BWR3',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': f"Basic {os.environ['FITBIT_AUTH_HEADER']}"}
        )
        os.environ['FITBIT_ACCESS_KEY'] = r.json()['access_token']
        os.environ['FITBIT_REFRESH_TOKEN'] = r.json()['refresh_token']
        print(r.json())
    return

