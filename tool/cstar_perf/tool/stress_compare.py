from benchmark import (bootstrap, stress, nodetool, nodetool_multi, cqlsh, bash, teardown,
                       log_stats, log_set_title, log_add_data, retrieve_logs, restart,
                       start_fincore_capture, stop_fincore_capture, retrieve_fincore_logs,
                       drop_page_cache, wait_for_compaction, setup_stress, clean_stress,
                       get_localhost, retrieve_flamegraph, retrieve_yourkit)
from benchmark import config as fab_config, cstar, dse, set_cqlsh_path, set_nodetool_path
import fab_common as common
import fab_cassandra as cstar
import fab_flamegraph as flamegraph
import fab_profiler as profiler
from fabric import api as fab
from fabric.tasks import execute
import os
import sys
import time
import datetime
import logging
import copy
import uuid
import argparse
import json
import subprocess
import shlex
import shutil
import sh

logging.basicConfig()
logger = logging.getLogger('stress_compare')
logger.setLevel(logging.INFO)

OPERATIONS = ['stress','nodetool','cqlsh','bash']

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

    # initial_destroy settting can be set in the job
    # configuration, or manually in the call to this function. Either
    # is fine, but they shouldn't conflict. If they do, ValueError is
    # raised.
    if initial_destroy == True and pristine_config.get('initial_destroy', None) == False:
        raise ValueError('setting for initial_destroy conflicts in job config and stress_compare() call')
    else:
        initial_destroy = pristine_config.get('initial_destroy', initial_destroy)

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

    for rev_num, revision_config in enumerate(revisions):
        config = copy.copy(pristine_config)
        config.update(revision_config)
        revision = revision_config['revision']
        config['log'] = log
        config['title'] = title
        config['subtitle'] = subtitle
        product = dse if config.get('product') == 'dse' else cstar

        # leave_data settting can be set in the revision
        # configuration, or manually in the call to this function.
        # Either is fine, but they shouldn't conflict. If they do,
        # ValueError is raised.
        if leave_data == True and revision_config.get('leave_data', None) == False:
            raise ValueError('setting for leave_data conflicts in job config and stress_compare() call')
        else:
            leave_data = revision_config.get('leave_data', leave_data)

        logger.info("Bringing up {revision} cluster...".format(revision=revision))

        # Drop the page cache between each revision, especially
        # important when leave_data=True :
        if not keep_page_cache:
            drop_page_cache()

        # Only fetch from git on the first run:
        git_fetch = True if rev_num == 0 else False
        revision_config['git_id'] = git_id = bootstrap(config, destroy=True, leave_data=leave_data, git_fetch=git_fetch)

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
                    logger.info("Cqlsh commands finished")

                elif operation['type'] == 'bash':
                    nodes = operation.get('nodes', [n for n in fab_config['hosts']])
                    logger.info("Running bash commands on: {nodes}".format(nodes=nodes))
                    stats['output'] = bash(operation['script'], nodes)
                    logger.info("Bash commands finished")

                end = datetime.datetime.now()
                stats['end_date'] = end.isoformat()
                stats['op_duration'] = str(end - start)
                log_stats(stats, file=log)
            finally:
                # Copy node logs:
                logs_dir = os.path.join(os.path.expanduser('~'),'.cstar_perf','logs')
                log_dir = os.path.join(logs_dir, stats['id'])
                os.makedirs(log_dir)
                retrieve_logs(log_dir)
                revision_config['last_log'] = stats['id']
                # Tar them for archiving:
                subprocess.Popen(shlex.split('tar cfvz {id}.tar.gz {id}'.format(id=stats['id'])), cwd=logs_dir).communicate()
                shutil.rmtree(log_dir)

            if capture_fincore:
                stop_fincore_capture()
                retrieve_fincore_logs(log_dir)
                # Restart fincore capture if this is not the last
                # operation:
                if operation_i < len(operations):
                    start_fincore_capture(interval=10)

        if flamegraph.is_enabled(revision_config):
            # Generate and Copy node flamegraphs
            execute(flamegraph.stop_perf_agent)
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
