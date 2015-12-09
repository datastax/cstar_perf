from benchmark import (bootstrap, stress, nodetool, teardown,
                       log_stats, log_set_title, retrieve_logs, config)
from fabric.tasks import execute
import fab_flamegraph as flamegraph
import fab_profiler as profiler
import fab_common as common
import argparse
import sys
import json
import copy

import logging
logging.basicConfig()
logger = logging.getLogger('bootstrap')
logger.setLevel(logging.INFO)

pristine_config = copy.copy(config)

flamegraph.set_common_module(common)
profiler.set_common_module(common)


def bootstrap_cluster(cfg):
    config = copy.copy(pristine_config)
    config.update(cfg)

    # Flamegraph Setup
    if flamegraph.is_enabled():
        execute(flamegraph.setup)

    git_id = bootstrap(config, destroy=True)
    return git_id

def main():
    parser = argparse.ArgumentParser(description='bootstrap')
    parser.add_argument('-v', metavar="GIT_REFSPEC",
                        help='The version of Cassandra to install, specified by git refspec (eg \'apache/cassandra-2.1\') - uses the default C* config. Use JSON_CONFIG file instead to change this.', dest="version")
    parser.add_argument('config', metavar="JSON_CONFIG",
                        help='The revision config JSON file', nargs='?', default=sys.stdin)
    args = parser.parse_args()

    if (not sys.stdin.isatty() and args.version):
        parser.print_help()
        print("\nYou can only specify a config file or a --version")
        exit(1)
        
    if args.version:
        bootstrap_cluster({'revision':args.version})
    else:
        if args.config == sys.stdin:
            if sys.stdin.isatty():
                print("You must specify a config file, a --version, or pipe a config to stdin")
                exit(1)
            cfg = json.load(sys.stdin)
        else:
            with open(args.config) as f:
                cfg = json.load(f)
        bootstrap_cluster(cfg)

if __name__ == "__main__":
    main()

