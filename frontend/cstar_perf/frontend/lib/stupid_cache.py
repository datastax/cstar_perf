"""Uses provided path on the local filesystem to cache binary data
"""
import hashlib
import os.path
import time

def stupid_cache_get( path, key, loader, expires, invalidated=False):
    digest = hashlib.sha1()
    digest.update(key)
    hexkey = digest.hexdigest()
    itempath = os.path.join(path, hexkey)

    mtime = 0
    exists = False
    if os.path.isfile(itempath):
        exists = True
        mtime = int(os.path.getmtime(itempath))

    expired = int(time.time()) - expires > mtime
    if expires == 0:
        expired = False

    if expired or invalidated or not exists:
        retval = loader()
        with open(itempath, 'w+b') as f:
            f.write(retval)
        return retval

    with open(itempath, 'rb') as f:
        return f.read()
