import string
import random
import requests

def random_token(length=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for x in xrange(length))

def download_file(url, dest, username=None, password=None):
    print("Downloading {} --> {}".format(url, dest))
    auth = (username, password) if username and password else None
    request = requests.get(url, auth=auth, stream=True)
    if request.status_code == requests.codes.ok:
        CHUNK = 16 * 1024
        with open(dest, 'wb') as fd:
            for chunk in request.iter_content(CHUNK):
                fd.write(chunk)
    return request
