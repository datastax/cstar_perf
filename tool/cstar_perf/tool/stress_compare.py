from benchmark import (bootstrap, stress, nodetool, nodetool_multi, cqlsh, bash, teardown, 
                       log_stats, log_set_title, log_add_data, retrieve_logs, cstar, restart,
                       start_fincore_capture, stop_fincore_capture, retrieve_fincore_logs,
                       drop_page_cache, wait_for_compaction)
from benchmark import config as fab_config
from fabric.tasks import execute
import os
import sys
import time
import datetime
import logging
import socket
import copy
import uuid
import argparse
import json
import subprocess
import shlex
import shutil

logging.basicConfig()
logger = logging.getLogger('stress_compare')
logger.setLevel(logging.INFO)

OPERATIONS = ['stress','nodetool','cqlsh']

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
            assert op.has_key('nodes'), "Nodetool operation missing nodes list"
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
         'command': 'write n=19000000 -rate threads=50',
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
    
    if initial_destroy:
        logger.info("Cleaning up from prior runs of stress_compare ...")
        teardown(destroy=True, leave_data=False)

    for rev_num, revision_config in enumerate(revisions):
        config = copy.copy(pristine_config)
        config.update(revision_config)
        revision = revision_config['revision']
        config['log'] = log
        config['title'] = title
        config['subtitle'] = subtitle

        logger.info("Bringing up {revision} cluster...".format(revision=revision))
        
        # Drop the page cache between each revision, especially 
        # important when leave_data=True : 
        if not keep_page_cache:
            drop_page_cache()

        #Only fetch from git on the first run:
        git_fetch = True if rev_num == 0 else False
        revision_config['git_id'] = git_id = bootstrap(config, destroy=True, leave_data=leave_data, git_fetch=git_fetch)
    
        if capture_fincore:
            start_fincore_capture(interval=10)

        for operation_i, operation in enumerate(operations, 1):
            start = datetime.datetime.now()
            stats = {"id":str(uuid.uuid1()), "type":operation['type'], 
                     "revision": revision, "git_id": git_id, "start_date":start.isoformat(),
                     "label":revision_config.get('label', revision_config['revision'])}

            if operation['type'] == 'stress':
                # Default to all the nodes of the cluster if no 
                # nodes were specified in the command:
                if operation.has_key('nodes'):
                    cmd = "{command} -node {hosts}".format(
                        command=operation['command'], 
                        hosts=",".join(host=operation['nodes']))
                elif '-node' in operation['command']:
                    cmd = operation['command']
                else:
                    cmd = "{command} -node {hosts}".format(
                        command=operation['command'], 
                        hosts=",".join([n for n in fab_config['hosts']]))
                stats['command'] = cmd
                stats['intervals'] = []
                stats['test'] = '{operation_i}_{operation}'.format(
                    operation_i=operation_i, operation=cmd.strip().split(' ')[0]).replace(" ","_")
                logger.info('Running stress operation : {cmd}  ...'.format(cmd=cmd))
                # Run stress:
                # (stress takes the stats as a parameter, and adds
                #  more as it runs):
                stats = stress(cmd, revision, stats)
                # Wait for all compactions to finish (unless disabled):
                if operation.get('wait_for_compaction', True):
                    compaction_throughput = revision_config.get("compaction_throughput_mb_per_sec", 16)
                    wait_for_compaction(compaction_throughput=compaction_throughput)

            elif operation['type'] == 'nodetool':
                if operation['nodes'] in ['all','ALL']:
                    nodes = [n for n in fab_config['hosts']]
                else:
                    nodes = operation['nodes']

                logger.info("Running nodetool on {nodes} with command: {command}".format(nodes=operation['nodes'], command=operation['command']))
                stats['command'] = operation['command']
                output = nodetool_multi(nodes, operation['command'])
                stats['output'] = output
                logger.info("Nodetool command finished on all nodes")

            elif operation['type'] == 'cqlsh':
                logger.info("Running cqlsh commands on {node}".format(node=operation['node']))
                output = cqlsh(operation['script'], operation['node'])
                stats['output'] = output.split("\n")
                logger.info("Cqlsh commands finished")

            elif operation['type'] == 'bash':
                nodes = operation.get('nodes', [n for n in fab_config['hosts']])
                logger.info("Running bash commands on {node}".format(nodes=nodes))
                output = bash(operation['script'], nodes)
                stats['output'] = output.split("\n")
                logger.info("Bash commands finished")


            end = datetime.datetime.now()
            stats['end_date'] = end.isoformat()
            stats['op_duration'] = str(end - start)
            log_stats(stats, file=log)

            #Copy node logs:
            logs_dir = os.path.join(os.path.expanduser('~'),'.cstar_perf','logs')
            log_dir = os.path.join(logs_dir, stats['id'])
            os.makedirs(log_dir)
            retrieve_logs(log_dir)
            revision_config['last_log'] = stats['id']
            #Tar them for archiving:
            subprocess.Popen(shlex.split('tar cfvz {id}.tar.gz {id}'.format(id=stats['id'])), cwd=logs_dir).communicate()
            shutil.rmtree(log_dir)

            if capture_fincore:
                stop_fincore_capture()
                retrieve_fincore_logs(log_dir)
                # Restart fincore capture if this is not the last
                # operation:
                if operation_i < len(operations):
                    start_fincore_capture(interval=10)

        log_add_data(log, {'title':title,
                           'subtitle': subtitle,
                           'revisions': revisions})

        teardown(destroy=True, leave_data=leave_data)

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
