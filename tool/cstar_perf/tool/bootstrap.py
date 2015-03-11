from benchmark import (bootstrap, stress, nodetool, teardown, 
                       log_stats, log_set_title, retrieve_logs, config)
from fabric.tasks import execute
import argparse
import json
import copy

import logging
logging.basicConfig()
logger = logging.getLogger('bootstrap')
logger.setLevel(logging.INFO)

pristine_config = copy.copy(config)

def bootstrap_cluster(cfg):
    config = copy.copy(pristine_config)
    config.update(cfg)

    git_id = bootstrap(config, destroy=True)
    return git_id

def main():
    parser = argparse.ArgumentParser(description='bootstrap')
    parser.add_argument('-v', metavar="GIT_REFSPEC",
                        help='The version of Cassandra to install, specified by git refspec (eg \'apache/cassandra-2.1\') - uses the default C* config. Use JSON_CONFIG file instead to change this.', dest="version")
    parser.add_argument('config', metavar="JSON_CONFIG",
                        help='The revision config JSON file', nargs='?')
    args = parser.parse_args()

    if not args.config and not args.version:
        parser.print_help()
        print("\nYou must specify a config file or a --version")
        exit(1)
    elif args.config and args.version:
        parser.print_help()
        print("\nYou can only specify a config file or a --version")
        exit(1)
    
    if args.version:
        bootstrap_cluster({'revision':args.version})
    else:
        with open(args.config) as f:
            bootstrap_cluster(json.loads(f.read()))
        

if __name__ == "__main__":
    main()

