import json
import os

import requests


def change_instance_state(turn_on=False):
    if turn_on:
        data = json.dumps({"settings": {'activationPolicy': 'ALWAYS'}})
        print('Turning instance on.\n')
    else:
        data = json.dumps({"settings": {'activationPolicy': 'NEVER'}})
        print('Turning instance off.\n')

    r = requests.patch(
        f'https://www.googleapis.com/sql/v1beta4/projects/{os.environ["GCLOUD_PROJECT_ID"]}/instances/' +
        f'{os.environ["GCLOUD_INSTANCE_ID"]}', data=data,
        headers={'Authorization': f'Bearer {os.environ["GCLOUD_ACCESS_TOKEN"]}',
                 'Content-Type': 'application/json; charset=utf-8'}
    )
    if r.json().get('status') == 'PENDING':
        print('Success!\n')
    elif r.json().get('error'):
        raise SystemExit(r.json()['error']['message'])


def main():
    change_instance_state(turn_on=False)


if __name__ == '__main__':
    main()
