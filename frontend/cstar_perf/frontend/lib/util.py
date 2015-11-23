import datetime
import hashlib
import random
import string
import signal
import time
import uuid
from contextlib import contextmanager
import os

def uuid_to_datetime(uid):
    return datetime.datetime.fromtimestamp((uid.get_time() - 0x01b21dd213814000L)*100/1e9)

def uuid_from_time(time_arg, node=None, clock_seq=None):
    """
    Converts a datetime or timestamp to a type 1 :class:`uuid.UUID`.

    :param time_arg:
      The time to use for the timestamp portion of the UUID.
      This can either be a :class:`datetime` object or a timestamp
      in seconds (as returned from :meth:`time.time()`).
    :type datetime: :class:`datetime` or timestamp

    :param node:
      None integer for the UUID (up to 48 bits). If not specified, this
      field is randomized.
    :type node: long

    :param clock_seq:
      Clock sequence field for the UUID (up to 14 bits). If not specified,
      a random sequence is generated.
    :type clock_seq: int

    :rtype: :class:`uuid.UUID`

    """
    if hasattr(time_arg, 'utctimetuple'):
        seconds = int(calendar.timegm(time_arg.utctimetuple()))
        microseconds = (seconds * 1e6) + time_arg.time().microsecond
    else:
        microseconds = int(time_arg * 1e6)

    # 0x01b21dd213814000 is the number of 100-ns intervals between the
    # UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
    intervals = int(microseconds * 10) + 0x01b21dd213814000

    time_low = intervals & 0xffffffff
    time_mid = (intervals >> 32) & 0xffff
    time_hi_version = (intervals >> 48) & 0x0fff

    if clock_seq is None:
        clock_seq = random.getrandbits(14)
    else:
        if clock_seq > 0x3fff:
            raise ValueError('clock_seq is out of range (need a 14-bit value)')

    clock_seq_low = clock_seq & 0xff
    clock_seq_hi_variant = 0x80 | ((clock_seq >> 8) & 0x3f)

    if node is None:
        node = random.getrandbits(48)

    return uuid.UUID(fields=(time_low, time_mid, time_hi_version,
                             clock_seq_hi_variant, clock_seq_low, node), version=1)

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


def sha256_of_file(path):
    """returns the SHA-256 hash in hex of the file"""
    h = hashlib.sha256()

    with open(path, 'rb') as fh:
        chunk = 0
        while chunk != b'':
            chunk = fh.read(512)
            h.update(chunk)

    return h.hexdigest()


def generate_object_id(test_id, kind, name):
    """returns the SHA-256 hash in hex of the test_id and kind"""

    return hashlib.sha256(kind + test_id + name).hexdigest()


def auth_provider_if_configured(config):
    if config.has_option('server', 'cassandra_user') and config.has_option('server', 'cassandra_password'):
        from cassandra.auth import PlainTextAuthProvider
        return PlainTextAuthProvider(username=config.get('server', 'cassandra_user'), password=config.get('server', 'cassandra_password'))
    return None
