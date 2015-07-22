import datetime
import random
import string
import signal
import time
from contextlib import contextmanager
import os

def uuid_to_datetime(uid):
    return datetime.datetime.fromtimestamp((uid.get_time() - 0x01b21dd213814000L)*100/1e9)

def random_token():
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for x in xrange(32))

class TimeoutError(Exception):
    pass

class timeout:
    """A context manager to timeout a block of code after x seconds

    >>> with timeout(3):
    ...     time.sleep(4)
    Traceback (most recent call last):
        ...
    TimeoutError: Timeout
    """
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message
    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
    def __exit__(self, type, value, traceback):
        signal.alarm(0)

def format_bytesize(num):
    """Convert number of bytes to human readable string"""
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')


def encode_unicode(
    obj, encoding='utf-8'):
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = unicode(obj, encoding)
    return obj

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(newdir)
    try:
        yield
    finally:
        os.chdir(prevdir)
