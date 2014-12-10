import requests
import json
from cstar_perf.frontend.lib.crypto import APIKey, BadConfigFileException

APP_ENDPOINT = 'http://localhost:8000/api'

def login():
    """Login to the server, return an authenticated requests session"""
    url = APP_ENDPOINT+"/login"
    client_key = APIKey.load(key_type='client')
    server_key = APIKey.load(key_type='server')
    # request a login token
    session = requests.Session()
    r = session.get(url)
    if r.status_code == 200:
        data = r.json()
        try:
            server_key.verify_message(data['token'], data['signature'])
        except Exception, e:
            raise RuntimeError('The server returned a bad signature for the token.', e)
    else:
        raise RuntimeError('Could not request login token : {} - {}'.format(r, r.text))

    # Sign the token and post it back:
    data = {'login': 'sarang',
            'signature': client_key.sign_message(data['token'])}
    r = session.post(url, data=json.dumps(data), headers={'content-type': 'application/json'})
    if r.json().get('success', '') != 'Logged in':
        raise RuntimeError('Login denied by server')
    return session

if __name__ == "__main__":
    login()
