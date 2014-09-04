import string
import random

def random_token(length=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for x in xrange(length))
