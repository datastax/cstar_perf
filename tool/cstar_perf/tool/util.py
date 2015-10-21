import string
import random
import requests
import hashlib
from static_vnode_tokens import get_token_group

def random_token(length=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for x in xrange(length))

def download_file(url, dest, username=None, password=None):
    "Download a file to disk"
    print("Downloading {} --> {}".format(url, dest))
    auth = (username, password) if username and password else None
    request = requests.get(url, auth=auth, stream=True)
    if request.status_code == requests.codes.ok:
        CHUNK = 16 * 1024
        with open(dest, 'wb') as fd:
            for chunk in request.iter_content(CHUNK):
                fd.write(chunk)
    else:
        request.raise_for_status()
    return request

def download_file_contents(url, username=None, password=None):
    "Download a file, return it's contents"
    print("Downloading {} ...".format(url))
    auth = (username, password) if username and password else None
    request = requests.get(url, auth=auth)
    if request.status_code == requests.codes.ok:
        return request.text
    else:
        request.raise_for_status()


def digest_file(path, blocksize=2**20, hash_method='sha1'):
    m = getattr(hashlib, hash_method)()
    with open(path,'rb') as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


def get_static_vnode_tokens(host, hosts, partitioner='murmur3', group='static'):
    host_position = sorted(hosts).index(host)
    host_ct = len(hosts)

    if host_ct > 12:
        raise NotImplementedError("static vnode tokens are only available for node counts <= 12")

    return get_token_group(partitioner, group)[host_position]
