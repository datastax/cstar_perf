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
    with fab.settings(warn_only=True):
        fab.get(os.path.join(flamegraph_directory, 'flamegraph_revision_{}.svg'.format(rev_num)), host_log_dir)
        fab.get(os.path.join(flamegraph_directory, 'perf_revision_{}.data'.format(rev_num)), host_log_dir)


@fab.parallel
def setup():
    logger.info("Setup Flamegraph dependencies")
    flamegraph_directory = get_flamegraph_directory()
    perf_map_agent_path, flamegraph_path = get_flamegraph_paths()
    java_home = common_module.config['jdk7_home']
    common_module.run_python_script(
        'flamegraph',
        'setup',
        '"{}", "{}", "{}", "{}"'.format(flamegraph_directory, flamegraph_path, perf_map_agent_path, java_home)
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

    flamegraph_directory = get_flamegraph_directory()
    fab.run('sudo chmod o+r {}'.format(os.path.join(flamegraph_directory, '*')))


@fab.parallel
def generate_flamegraph(rev_num):
    logger.info("Generate Flamegraph")
    perf_map_agent_path, flamegraph_path = get_flamegraph_paths()
    java_home = common_module.config['jdk7_home']
    flamegraph_directory = get_flamegraph_directory()

    perf_data_file = os.path.join(flamegraph_directory, 'perf_revision_{}.data'.format(rev_num + 1))
    flamegraph_file = os.path.join(flamegraph_directory, 'flamegraph_revision_{}.svg'.format(rev_num + 1))
    stacks_file = os.path.join(flamegraph_directory, 'perf.stacks')
    collapsed_file = os.path.join(flamegraph_directory, 'perf.collapsed')

    # Generate java symbol map
    output = common_module.run_python_script(
        'utils',
        'find_process_pid',
        '"{}"'.format("java.*Cassandra")
    )
    cassandra_pids = common_module.parse_output(output)
    for host, pid in cassandra_pids.iteritems():
        with common_module.fab.settings(hosts=[host]):
            fab.run('PATH={java_home}/bin:$PATH JAVA_HOME={java_home} {perf_path}/bin/create-java-perf-map.sh {pid}'.format(
                java_home=java_home, perf_path=perf_map_agent_path, pid=pid[0]))

    fab.run('sudo perf script -i {} > {}'.format(perf_data_file, stacks_file))
    fab.run(('{fg_path}/stackcollapse-perf.pl {stacks} | '
             'tee {collapsed} | {fg_path}/flamegraph.pl --color=java > {fg_output}'
         ).format(fg_dir=flamegraph_directory, fg_output=flamegraph_file,
                  stacks=stacks_file, collapsed=collapsed_file, fg_path=flamegraph_path))
    fab.run('sudo chmod o+r {}'.format(os.path.join(flamegraph_directory, '*')))


@fab.parallel
def start_perf_agent(rev_num):
    logger.info("Starting Flamegraph perf agent")
    perf_map_agent_path, flamegraph_path = get_flamegraph_paths()
    java_home = common_module.config['jdk7_home']
    output = common_module.run_python_script(
        'utils',
        'find_process_pid',
        '"{}"'.format("java.*Cassandra")
    )
    cassandra_pids = common_module.parse_output(output)
    perf_bin = os.path.join(os.path.expanduser('~/fab/perf-map-agent'), 'bin/perf-java-flames')
    flamegraph_directory = get_flamegraph_directory()

    env_vars = {
        'PATH': "{}:{}".format(os.path.join(java_home, 'bin'), "$PATH"),
        'JAVA_HOME': java_home,
        'FLAMEGRAPH_DIR': flamegraph_path,
        'PERF_RECORD_SECONDS': PERF_RECORDING_DURATION,
        'PERF_JAVA_TMP': flamegraph_directory,
        'PERF_DATA_FILE': os.path.join(flamegraph_directory, 'perf_revision_{}.data'.format(rev_num + 1)),
        'PERF_FLAME_OUTPUT': os.path.join(flamegraph_directory, 'flamegraph_revision_{}.svg'.format(rev_num + 1)),
        'STACKS': "{}/perf.stacks".format(flamegraph_directory),
        'COLLAPSED': "{}/perf.collapsed".format(flamegraph_directory)
    }

    for host, pid in cassandra_pids.iteritems():
        with common_module.fab.settings(hosts=[host]):
            common_module.runbg("{perf_bin} {pid}".format(perf_bin=perf_bin, pid=pid[0]), env_vars)
