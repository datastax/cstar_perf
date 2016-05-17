from benchmark import (bootstrap, stress, nodetool, nodetool_multi, cqlsh, bash, teardown,
                       log_stats, log_set_title, log_add_data, retrieve_logs, restart,
                       start_fincore_capture, stop_fincore_capture, retrieve_fincore_logs,
                       drop_page_cache, wait_for_compaction, setup_stress, clean_stress,
                       get_localhost, retrieve_flamegraph, retrieve_yourkit, dsetool_cmd, dse_cmd, CSTAR_PERF_LOGS_DIR)
from benchmark import config as fab_config, cstar, dse, set_cqlsh_path, set_nodetool_path, spark_cassandra_stress, retrieve_logs_and_create_tarball
import fab_common as common
import fab_cassandra as cstar
import fab_flamegraph as flamegraph
import fab_profiler as profiler
from fabric.tasks import execute
from command import Ctool
import os
import sys
import datetime
import logging
import copy
import uuid
import argparse
import json
import shutil
import sh
import distutils.util
import signal


logging.basicConfig()
logger = logging.getLogger('stress_compare')
logger.setLevel(logging.INFO)

OPERATIONS = ['stress','nodetool','cqlsh','bash', 'ctool', 'spark_cassandra_stress', 'dse', 'dsetool']

flamegraph.set_common_module(common)
profiler.set_common_module(common)


def validate_revisions_list(revisions):
    for rev in revisions:
        assert rev.has_key('revision'), "Revision needs a 'revision' tag"

def validate_operations_list(operations):
    """Spot check a list of operations for required parameters"""
    for op in operations:
        assert op.has_key('type'), "Operation {op} needs a type".format(op=op)
        assert op['type'] in OPERATIONS, "Unknown operation '{type}'".format(type=op['type'])
        if op['type'] == 'stress':
            assert op.has_key('command'), "Stress operation missing comamnd"
            if op.has_key('node'):
                raise AssertionError('stress operations use a nodes parameter, not node.')
            elif op.has_key('nodes'):
                assert '-node' not in op['command'], "Stress command line cannot specify nodes if nodes is specified in operation"
        elif op['type'] == 'nodetool':
            assert op.has_key('command'), "Nodetool operation missing comamnd"
            for option in [' -h',' --host']:
                assert option not in op['command'], "Nodetool command cananot specify the host, use the nodes parameter in the operation instead"
        elif op['type'] == 'cqlsh':
            assert op.has_key('script'), "Cqlsh operation missing script"
            assert op.has_key('node'), "Cqlsh operation missing node to run on"
        elif op['type'] == 'bash':
            assert op.has_key('script'), "Bash operation missing script"
        elif op['type'] == 'spark_cassandra_stress':
            assert op.has_key('script'), "spark_cassandra_stress is missing parameters"
            assert op.has_key('node'), "spark_cassandra_stress missing node to run on"
        elif op['type'] == 'ctool':
            assert op.has_key('command'), "ctool is missing parameters"


class GracefulTerminationHandler(object):
    """
    Adapted from https://gist.github.com/nonZero/2907502.

    Will detect a graceful termination and try to grab system.logs / stats files / flamegraphs.
    """

    def __init__(self, sig=signal.SIGTERM):
        self.sig = sig

    def __enter__(self):
        logger.info('GracefulTerminationHandler -- __enter__')
        self.terminated = False
        self.released = False
        self.original_handler = signal.getsignal(self.sig)

        def handler(signum, frame):
            self.release()
            self.terminated = True

        signal.signal(self.sig, handler)
        return self

    def __exit__(self, type, value, tb):
        logger.info('GracefulTerminationHandler -- __exit__')
        self.release()

    def release(self):
        if self.released:
            return False

        signal.signal(self.sig, self.original_handler)
        self.released = True
        return True


def stress_compare(revisions,
                   title,
                   log,
                   operations = [],
                   subtitle = '',
                   capture_fincore=False,
                   initial_destroy=True,
                   leave_data=False,
                   keep_page_cache=False
               ):
    """
    Run Stress on multiple C* branches and compare them.

    revisions - List of dictionaries that contain cluster configurations
                to trial. This is combined with the default config.
    title - The title of the comparison
    subtitle - A subtitle for more information (displayed smaller underneath)
    log - The json file path to record stats to
    operations - List of dictionaries indicating the operations. Example:
       [# cassandra-stress command, node defaults to cluster defined 'stress_node'
        {'type': 'stress',
         'command': 'write n=19M -rate threads=50',
         'node': 'node1',
         'wait_for_compaction': True},
        # nodetool command to run in parallel on nodes:
        {'type': 'nodetool',
         'command': 'decomission',
         'nodes': ['node1','node2']},
        # cqlsh script, node defaults to cluster defined 'stress_node'
        {'type': 'cqlsh',
         'script': "use my_ks; INSERT INTO blah (col1, col2) VALUES (val1, val2);",
         'node': 'node1'}
       ]
    capture_fincore - Enables capturing of linux-fincore logs of C* data files.
    initial_destroy - Destroy all data before the first revision is run.
    leave_data - Whether to leave the Cassandra data/commitlog/etc directories intact between revisions.
    keep_page_cache - Whether to leave the linux page cache intact between revisions.
    """
    validate_revisions_list(revisions)
    validate_operations_list(operations)

    pristine_config = copy.copy(fab_config)

    # initial_destroy setting can be set in the job
    # configuration, or manually in the call to this function.
    # Either is fine, but they shouldn't conflict. If they do,
    # ValueError is raised.
    if initial_destroy == True and pristine_config.get('initial_destroy', None) == False:
        raise ValueError('setting for initial_destroy conflicts in job config and stress_compare() call')
    else:
        initial_destroy = bool(distutils.util.strtobool(str(pristine_config.get('initial_destroy', initial_destroy))))

    if initial_destroy:
        logger.info("Cleaning up from prior runs of stress_compare ...")
        teardown(destroy=True, leave_data=False)

    # Update our local cassandra git remotes and branches
    _, localhost_entry = get_localhost()
    with common.fab.settings(hosts=[localhost_entry]):
        execute(cstar.update_cassandra_git)

    # Flamegraph Setup
    if flamegraph.is_enabled():
        execute(flamegraph.setup)

    clean_stress()
    stress_revisions = set([operation['stress_revision'] for operation in operations if 'stress_revision' in operation])
    stress_shas = setup_stress(stress_revisions)

    with GracefulTerminationHandler() as handler:
        for rev_num, revision_config in enumerate(revisions):
            config = copy.copy(pristine_config)
            config.update(revision_config)
            revision = revision_config['revision']
            config['log'] = log
            config['title'] = title
            config['subtitle'] = subtitle
            product = dse if config.get('product') == 'dse' else cstar

            # leave_data setting can be set in the revision
            # configuration, or manually in the call to this function.
            # Either is fine, but they shouldn't conflict. If they do,
            # ValueError is raised.
            if leave_data == True and revision_config.get('leave_data', None) == False:
                raise ValueError('setting for leave_data conflicts in job config and stress_compare() call')
            else:
                leave_data = bool(distutils.util.strtobool(str(revision_config.get('leave_data', leave_data))))

            logger.info("Bringing up {revision} cluster...".format(revision=revision))

            # Drop the page cache between each revision, especially
            # important when leave_data=True :
            if not keep_page_cache:
                drop_page_cache()

            # Only fetch from git on the first run:
            git_fetch = True if rev_num == 0 else False
            revision_config['git_id'] = git_id = bootstrap(config,
                                                           destroy=initial_destroy,
                                                           leave_data=leave_data,
                                                           git_fetch=git_fetch)

            if flamegraph.is_enabled(revision_config):
                execute(flamegraph.ensure_stopped_perf_agent)
                execute(flamegraph.start_perf_agent, rev_num)

            if capture_fincore:
                start_fincore_capture(interval=10)

            last_stress_operation_id = 'None'
            for operation_i, operation in enumerate(operations, 1):
                try:
                    start = datetime.datetime.now()
                    stats = {
                        "id": str(uuid.uuid1()),
                        "type": operation['type'],
                        "revision": revision,
                        "git_id": git_id,
                        "start_date": start.isoformat(),
                        "label": revision_config.get('label', revision_config['revision']),
                        "test": '{operation_i}_{operation}'.format(
                            operation_i=operation_i,
                            operation=operation['type'])
                    }

                    if operation['type'] == 'stress':
                        last_stress_operation_id = stats['id']
                        # Default to all the nodes of the cluster if no
                        # nodes were specified in the command:
                        if operation.has_key('nodes'):
                            cmd = "{command} -node {hosts}".format(
                                command=operation['command'],
                                hosts=",".join(operation['nodes']))
                        elif '-node' in operation['command']:
                            cmd = operation['command']
                        else:
                            cmd = "{command} -node {hosts}".format(
                                command=operation['command'],
                                hosts=",".join([n for n in fab_config['hosts']]))
                        stats['command'] = cmd
                        stats['intervals'] = []
                        stats['test'] = '{operation_i}_{operation}'.format(
                            operation_i=operation_i, operation=cmd.strip().split(' ')[0]).replace(" ", "_")
                        logger.info('Running stress operation : {cmd}  ...'.format(cmd=cmd))
                        # Run stress:
                        # (stress takes the stats as a parameter, and adds
                        #  more as it runs):
                        stress_sha = stress_shas[operation.get('stress_revision', 'default')]
                        stats = stress(cmd, revision, stress_sha, stats=stats)
                        # Wait for all compactions to finish (unless disabled):
                        if operation.get('wait_for_compaction', True):
                            compaction_throughput = revision_config.get("compaction_throughput_mb_per_sec", 16)
                            wait_for_compaction(compaction_throughput=compaction_throughput)

                    elif operation['type'] == 'nodetool':
                        if 'nodes' not in operation:
                            operation['nodes'] = 'all'
                        if operation['nodes'] in ['all','ALL']:
                            nodes = [n for n in fab_config['hosts']]
                        else:
                            nodes = operation['nodes']

                        set_nodetool_path(os.path.join(product.get_bin_path(), 'nodetool'))
                        logger.info("Running nodetool on {nodes} with command: {command}".format(nodes=operation['nodes'], command=operation['command']))
                        stats['command'] = operation['command']
                        output = nodetool_multi(nodes, operation['command'])
                        stats['output'] = output
                        logger.info("Nodetool command finished on all nodes")

                    elif operation['type'] == 'cqlsh':
                        logger.info("Running cqlsh commands on {node}".format(node=operation['node']))
                        set_cqlsh_path(os.path.join(product.get_bin_path(), 'cqlsh'))
                        output = cqlsh(operation['script'], operation['node'])
                        stats['output'] = output.split("\n")
                        stats['command'] = operation['script']
                        logger.info("Cqlsh commands finished")

                    elif operation['type'] == 'bash':
                        nodes = operation.get('nodes', [n for n in fab_config['hosts']])
                        logger.info("Running bash commands on: {nodes}".format(nodes=nodes))
                        stats['output'] = bash(operation['script'], nodes)
                        stats['command'] = operation['script']
                        logger.info("Bash commands finished")

                    elif operation['type'] == 'spark_cassandra_stress':
                        node = operation['node']
                        logger.info("Running spark_cassandra_stress on {node}".format(node=node))
                        output = spark_cassandra_stress(operation['script'], node)
                        stats['output'] = output
                        logger.info("spark_cassandra_stress finished")

                    elif operation['type'] == 'ctool':
                        logger.info("Running ctool with parameters: {command}".format(command=operation['command']))
                        ctool = Ctool(operation['command'], common.config)
                        output = execute(ctool.run)
                        stats['output'] = output
                        logger.info("ctool finished")

                    elif operation['type'] == 'dsetool':
                        if 'nodes' not in operation:
                            operation['nodes'] = 'all'
                        if operation['nodes'] in ['all','ALL']:
                            nodes = [n for n in fab_config['hosts']]
                        else:
                            nodes = operation['nodes']

                        dsetool_options = operation['script']
                        logger.info("Running dsetool {command} on {nodes}".format(nodes=operation['nodes'], command=dsetool_options))
                        stats['command'] = dsetool_options
                        output = dsetool_cmd(nodes=nodes, options=dsetool_options)
                        stats['output'] = output
                        logger.info("dsetool command finished on all nodes")

                    elif operation['type'] == 'dse':
                        logger.info("Running dse command on {node}".format(node=operation['node']))
                        output = dse_cmd(node=operation['node'], options=operation['script'])
                        stats['output'] = output.split("\n")
                        stats['command'] = operation['script']
                        logger.info("dse commands finished")

                    end = datetime.datetime.now()
                    stats['end_date'] = end.isoformat()
                    stats['op_duration'] = str(end - start)
                    log_stats(stats, file=log)
                finally:
                    # Copy node logs:
                    retrieve_logs_and_create_tarball(job_id=stats['id'])
                    revision_config['last_log'] = stats['id']

                if capture_fincore:
                    stop_fincore_capture()
                    log_dir = os.path.join(CSTAR_PERF_LOGS_DIR, stats['id'])
                    retrieve_fincore_logs(log_dir)
                    # Restart fincore capture if this is not the last
                    # operation:
                    if operation_i < len(operations):
                        start_fincore_capture(interval=10)

            if flamegraph.is_enabled(revision_config):
                # Generate and Copy node flamegraphs
                execute(flamegraph.stop_perf_agent)
                execute(flamegraph.generate_flamegraph, rev_num)
                flamegraph_dir = os.path.join(os.path.expanduser('~'),'.cstar_perf', 'flamegraph')
                flamegraph_test_dir = os.path.join(flamegraph_dir, last_stress_operation_id)
                retrieve_flamegraph(flamegraph_test_dir, rev_num+1)
                sh.tar('cfvz', "{}.tar.gz".format(stats['id']), last_stress_operation_id, _cwd=flamegraph_dir)
                shutil.rmtree(flamegraph_test_dir)

            log_add_data(log, {'title':title,
                               'subtitle': subtitle,
                               'revisions': revisions})

            if revisions[-1].get('leave_data', leave_data):
                teardown(destroy=False, leave_data=True)
            else:
                kill_delay = 300 if profiler.yourkit_is_enabled(revision_config) else 0
                teardown(destroy=True, leave_data=False, kill_delay=kill_delay)

            if profiler.yourkit_is_enabled(revision_config):
                yourkit_config = profiler.yourkit_get_config()
                yourkit_dir = os.path.join(os.path.expanduser('~'),'.cstar_perf', 'yourkit')
                yourkit_test_dir = os.path.join(yourkit_dir, last_stress_operation_id)
                retrieve_yourkit(yourkit_test_dir, rev_num+1)
                sh.tar('cfvz', "{}.tar.gz".format(stats['id']),
                       last_stress_operation_id, _cwd=yourkit_dir)
                shutil.rmtree(yourkit_test_dir)


def main():
    parser = argparse.ArgumentParser(description='stress_compare')
    parser.add_argument('configs', metavar="CONFIG",
                        help='JSON config file(s) containing stress_compare() arguments, use - to read from stdin',
                        nargs="+")
    args = parser.parse_args()

    # parse config files:
    configs = []
    for cfg_file in args.configs:
        if cfg_file == "-":
            f = sys.stdin
        else:
            f = open(cfg_file)
        cfg = json.loads(f.read())
        configs.append(cfg)
        f.close()

    # run each config:
    for cfg in configs:
        stress_compare(**cfg)

if __name__ == "__main__":
    main()
