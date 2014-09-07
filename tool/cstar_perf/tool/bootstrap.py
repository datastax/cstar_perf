from benchmark import (bootstrap, stress, nodetool, teardown, 
                       log_stats, log_set_title, retrieve_logs, cstar, config)
from fabric.tasks import execute
import argparse

import logging
logging.basicConfig()
logger = logging.getLogger('bootstrap')
logger.setLevel(logging.INFO)


def bootstrap_cluster(revision, override_version=None):
    """
    Bootstrap a C* revision on the cluster
    """
    config['revision'] = revision
    config['override_version'] = override_version
    config['use_vnodes'] = True

    logger.info("Bringing up {revision} cluster...".format(revision=revision))
    bootstrap(config, destroy=True)



def main():
    parser = argparse.ArgumentParser(description='bootstrap')
    parser.add_argument('version', metavar="GIT_REFSPEC",
                        help='The version of Cassandra to install, specified by git refspec (eg \'apache/cassandra-2.1\')')
    args = parser.parse_args()

    bootstrap_cluster(revision=args.version)
        

if __name__ == "__main__":
    main()

