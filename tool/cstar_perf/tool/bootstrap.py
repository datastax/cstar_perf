from benchmark import (bootstrap, stress, nodetool, teardown, 
                       log_stats, log_set_title, retrieve_logs, cstar, config)
from fabric.tasks import execute

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

if __name__ == "__main__":

    bootstrap_cluster(revision='apache/cassandra-2.1')
