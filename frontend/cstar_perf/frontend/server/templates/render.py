# Global list of available format parsers on your system
# mapped to the callable/Exception to parse a string into a dict
formats = {}

class MalformedJSON(Exception): pass

# json - simplejson or packaged json as a fallback
try:
    import simplejson
    formats['json'] = (simplejson.loads, simplejson.decoder.JSONDecodeError, MalformedJSON)
except ImportError:
    try:
        import json
        formats['json'] = (json.loads, ValueError, MalformedJSON)
    except ImportError:
        pass

import os
import sys
from optparse import OptionParser

from jinja2 import Environment, FileSystemLoader

def cli(opts, args):
    if args[1] == '-':
        data = sys.stdin.read()
    else:
        data = open(os.path.join(os.getcwd(), os.path.expanduser(args[1]))).read()

    try:
        data = formats['json'][0](data)
    except formats['json'][1]:
        raise formats['json'][2](u'%s ...' % data[:60])
        sys.exit(1)

    env = Environment(loader=FileSystemLoader(os.getcwd()))
    sys.stdout.write(env.get_template(args[0]).render(data))
    sys.exit(0)


def main():
    default_format = 'json'
    if default_format not in formats:
        default_format = sorted(formats.keys())[0]

    parser = OptionParser(usage="usage: %prog [options] <input template> <input data>")
    opts, args = parser.parse_args()

    if len(args) == 0:
        parser.print_help()
        sys.exit(1)

    # Without the second argv, assume they want to read from stdin
    if len(args) == 1:
        args.append('-')

    cli(opts, args)

if __name__ == "__main__":
    main()
