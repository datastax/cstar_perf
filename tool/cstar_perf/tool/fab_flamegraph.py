"""
Fabric file to bootstrap Profiling Flamegraph on a set of nodes
"""

import os
import logging
import time
import sh
from fabric import api as fab
from distutils import version as v

logging.basicConfig()
logger = logging.getLogger('flamegraph')
logger.setLevel(logging.INFO)

# used internally, perf recording is stopped using SIGINT
PERF_RECORDING_DURATION = 432000  # 5 days
common_module = None


def set_common_module(module):
    global common_module
    common_module = module


def is_enabled(revision_config=None):
    is_compatible = True
    is_enabled = common_module.config.get('flamegraph', False)
    if revision_config:
        if revision_config.get('product', 'cassandra') == 'dse':
            logger.info('Flamegraph is not compatible with DSE yet')
            is_compatible = False
        jvm = revision_config.get('java_home', '')
        try:
            jvm = os.path.basename(jvm)
            jvm = jvm[jvm.index('1'):]
            if v.LooseVersion(jvm) < v.LooseVersion('1.8.0_60'):
                logger.info('Flamegraph is not compatible with java <1.8.0_60')
                is_compatible = False
        except ValueError:
            pass
    return is_enabled and is_compatible


def get_flamegraph_paths():
    perf_map_agent_path = os.path.expanduser('~/fab/perf-map-agent')
    flamegraph_path = os.path.expanduser('~/fab/flamegraph')

    return (perf_map_agent_path, flamegraph_path)


def get_flamegraph_directory():
    return common_module.config.get('flamegraph_directory', '/tmp/flamegraph')


@fab.parallel
def copy_flamegraph(local_directory, rev_num):
    logger.info("Copying Flamegraph data")
    cfg = common_module.config['hosts'][fab.env.host]
    host_log_dir = os.path.join(local_directory, cfg['hostname'])
    flamegraph_directory = get_flamegraph_directory()
    os.makedirs(host_log_dir)
    fab.get(os.path.join(flamegraph_directory, 'flamegraph_revision_{}.svg'.format(rev_num)), host_log_dir)
    fab.get(os.path.join(flamegraph_directory, 'perf_revision_{}.data'.format(rev_num)), host_log_dir)


@fab.parallel
def setup():
    logger.info("Setup Flamegraph dependencies")
    flamegraph_directory = get_flamegraph_directory()
    perf_map_agent_path, flamegraph_path = get_flamegraph_paths()
    common_module.run_python_script(
        'flamegraph',
        'setup',
        '"{}", "{}", "{}"'.format(flamegraph_directory, flamegraph_path, perf_map_agent_path)
    )


@fab.parallel
def ensure_stopped_perf_agent():
    logger.info("Ensure there are no perf agent running")
    common_module.run_python_script('flamegraph', 'ensure_stopped_perf_agent', '')


@fab.parallel
def stop_perf_agent():
    logger.info("Stopping Flamegraph perf agent")
    common_module.run_python_script(
        'utils',
        'find_and_kill_process',
        '"{}", {}'.format("[^/]perf record -F", 'child_process=True')
    )

    logger.info("Waiting 10 seconds to for the flamegraph generation")
    time.sleep(10)
    flamegraph_directory = get_flamegraph_directory()
    fab.run('sudo chmod o+r {}'.format(os.path.join(flamegraph_directory, '*')))


@fab.parallel
def start_perf_agent(rev_num):
    logger.info("Starting Flamegraph perf agent")
    perf_map_agent_path, flamegraph_path = get_flamegraph_paths()
    java_home = os.path.expanduser(common_module.config['java_home'])
    output = common_module.run_python_script(
        'utils',
        'find_process_pid',
        '"{}"'.format("java.*Cassandra")
    )
    cassandra_pids = common_module.parse_output(output)
    perf_bin = os.path.join(os.path.expanduser('~/fab/perf-map-agent'), 'bin/perf-java-flames')
    flamegraph_directory = get_flamegraph_directory()

    env_vars = {
        'JAVA_HOME': java_home,
        'FLAMEGRAPH_DIR': flamegraph_path,
        'PERF_RECORD_SECONDS': PERF_RECORDING_DURATION,
        'PERF_JAVA_TMP': flamegraph_directory,
        'PERF_DATA_FILE': os.path.join(flamegraph_directory, 'perf_revision_{}.data'.format(rev_num + 1)),
        'PERF_FLAME_OUTPUT': os.path.join(flamegraph_directory, 'flamegraph_revision_{}.svg'.format(rev_num + 1))
    }

    for host, pid in cassandra_pids.iteritems():
        with common_module.fab.settings(hosts=[host]):
            common_module.runbg("{perf_bin} {pid}".format(perf_bin=perf_bin, pid=pid[0]), env_vars)
