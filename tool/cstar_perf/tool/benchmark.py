"""
Bootstrap Cassandra onto a cluster and benchmark stress.
"""

import argparse
import shlex
import subprocess
import tempfile
import os, sys
import time
import datetime
from pprint import pprint
from fabric.tasks import execute
import uuid
import re
import json
import threading
import socket
import getpass
import logging
import yaml
import sh
import itertools

# Import the default config first:r
import fab_common as common
import fab_dse as dse
import fab_cassandra as cstar

# Then import our cluster specific config:
from cluster_config import config

logging.basicConfig()
logger = logging.getLogger('benchmark')
logger.setLevel(logging.INFO)

# Ensure stdout is not truncated when a sh.Command fails
sh.ErrorReturnCode.truncate_cap = 999999

HOME = os.getenv('HOME')
CASSANDRA_STRESS_PATH = os.path.expanduser("~/fab/stress/")
CASSANDRA_STRESS_DEFAULT   = os.path.expanduser("~/fab/stress/default/tools/bin/cassandra-stress")
JAVA_HOME          = os.path.expanduser("~/fab/java")

antcmd = sh.Command(os.path.join(HOME, 'fab/ant/bin/ant'))

global nodetool_path, cqlsh_path

def set_nodetool_path(path):
    global nodetool_path
    nodetool_path = path

def set_cqlsh_path(path):
    global cqlsh_path
    cqlsh = path

def bootstrap(cfg=None, destroy=False, leave_data=False, git_fetch=True):
    """Deploy and start cassandra on the cluster
    
    cfg - the cluster configuration
    destroy - whether to destroy the existing build before bootstrap
    leave_data - if destroy==True, leave the Cassandra data/commitlog/etc directories intact.
    git_fetch - Do a git fetch before building/running C*? (Multi-revision tests should only update on the first run to maintain revision consistency in case someone checks something in mid-operation.)

    Return the gid id of the branch checked out
    """
    if cfg is not None:
        common.setup(cfg)

    # Parse yaml 
    if cfg.has_key('yaml'):
        cass_yaml = cfg['yaml']
        if isinstance(cass_yaml, basestring):
            cass_yaml = yaml.load(cass_yaml)
        if cass_yaml is None:
            cass_yaml = {}
        if type(cass_yaml) is not dict:
            raise JobFailure('Invalid yaml, was expecting a dictionary: {cass_yaml}'.format(cass_yaml=cass_yaml))
        common.config['yaml'] = cass_yaml
    if cfg.has_key('options'):
        if cfg['options'] is not None:
            common.config.update(cfg['options'])
            del common.config['options']

    logger.info("### Config: ###")
    pprint(common.config)

    # leave_data settting can be set in the revision
    # configuration, or manually in the call to this function.
    # Either is fine, but they shouldn't conflict. If they do,
    # ValueError is raised.
    if leave_data == True and cfg.get('leave_data', None) == False:
        raise ValueError('setting for leave_data conflicts in job config and bootstrap() call')
    else:
        leave_data = cfg.get('leave_data', leave_data)

    # Set device readahead:
    if cfg['blockdev_readahead'] is not None:
        if len(cfg['block_devices']) == 0:
            raise AssertionError('blockdev_readahead setting requires block_devices to be set in cluster_config.')
        set_device_read_ahead(cfg['blockdev_readahead'])

    # Destroy cassandra deployment and data:
    if destroy:
        execute(common.destroy, leave_data=leave_data)
        execute(common.ensure_stopped)
    else:
        #Shutdown cleanly:
        execute(common.stop)
        execute(common.ensure_stopped)

    product = dse if common.config['product'] == 'dse' else cstar

    # dse setup and binaries download (local)
    if product == dse:
        dse.setup(common.config)

    set_nodetool_path(os.path.join(product.get_bin_path(), 'nodetool'))
    set_cqlsh_path(os.path.join(product.get_bin_path(), 'cqlsh'))

    # Bootstrap C* onto the cluster nodes, as well as the localhost,
    # so we have access to nodetool, stress etc - the local host will
    # not be added to the cluster unless it has a corresponding entry
    # in the cluster config:
    hosts = list(common.fab.env['hosts'])
    localhost = socket.gethostname().split(".")[0]
    if localhost not in [host.split(".")[0] for host in hosts]:
        # Use the local username for this host, as it may be different
        # than the cluster defined 'user' parameter:
        hosts += [getpass.getuser() + "@" + localhost]
    if not cfg.get('revision_override'):
        with common.fab.settings(hosts=hosts):
            git_ids = execute(common.bootstrap, git_fetch=git_fetch)
    else:
        # revision_override is only supported for the product cassandra
        if product.name != 'cassandra':
            raise ValueError("Cannot use revision_override for product: {}".format(
                product.name))
        git_ids = {}
        default_hosts = set(hosts) - set(itertools.chain(*cfg['revision_override'].values()))
        print 'default version on {default_hosts}'.format(default_hosts=default_hosts)
        with common.fab.settings(hosts=default_hosts):
            git_ids.update(execute(common.bootstrap, git_fetch=git_fetch))
        for override_revision, hosts_to_override in cfg['revision_override'].items():
            print '{revision} on {hosts_to_override}'.format(revision=override_revision, hosts_to_override=hosts_to_override)
            with common.fab.settings(hosts=hosts_to_override):
                git_ids.update(execute(common.bootstrap, git_fetch=git_fetch, revision_override=override_revision))

    if product.name == 'cassandra':
        overridden_host_versions = {}
        for v, hs in cfg.get('revision_override', {}).items():
            overridden_host_versions.update({h: v for h in hs})
        expected_host_versions = dict({h: cfg['revision'] for h in hosts}, **overridden_host_versions)
        expected_host_shas = {h: str(sh.git('--git-dir={home}/fab/cassandra.git'.format(home=HOME), 'rev-parse', v))
                              for (h, v) in expected_host_versions.items()}
        expected_host_shas = {h: v.strip() for (h, v) in expected_host_shas.items()}

        assert expected_host_shas == git_ids, 'expected: {}\ngot:{}'.format(expected_host_shas, git_ids)

    execute(common.start)
    execute(common.ensure_running, hosts=[common.config['seeds'][0]])
    time.sleep(30)

    logger.info("Started {product} on {n} nodes with git SHAs: {git_ids}".format(
        product=product.name, n=len(common.fab.env['hosts']), git_ids=git_ids))
    time.sleep(30)
    return git_ids

def restart():
    execute(common.stop)
    execute(common.ensure_stopped)
    execute(common.start)
    execute(common.ensure_running)

def teardown(destroy=False, leave_data=False):
    if destroy:
        execute(common.destroy, leave_data=leave_data)
    else:
        execute(common.stop)
        execute(common.ensure_stopped)

class NodetoolException(Exception):
    pass
        
def nodetool(cmd):
    """Run a nodetool command
    
    Raises NodetoolException if we can't connect or another error occurs:
    """
    cmd = "JAVA_HOME={JAVA_HOME} {nodetool_path} {cmd}".format(
        JAVA_HOME=JAVA_HOME, nodetool_path=nodetool_path, cmd=cmd)
    proc = subprocess.Popen(cmd, 
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            shell=True)
    output = proc.communicate()
    if proc.returncode != 0:
        raise NodetoolException(output)
    return output[0]

def bash(script, nodes=None, user=None):
    """Run a bash script on a set of nodes
    
    script - A bash script written as a string or list.
    nodes  - The set of nodes to run the command on. If None, all nodes of 
             the cluster will be used.
    user   - The user to run the command as. If None, the default user specified 
             in the cluster configuration    
    """ 
    if type(script) in (list, tuple):
        script = "\n".join(script)
    if nodes is None:
        nodes = common.fab.env.hosts
    if user is None:
        user = common.fab.env.user
    with common.fab.settings(user=user, hosts=nodes):
        execute(common.bash, script)

def cqlsh(script, node):
    """Run a cqlsh script on a node"""
    cmd = "{cqlsh_path} --no-color {host}".format(cqlsh_path=cqlsh_path, host=node)
    proc = subprocess.Popen(shlex.split(cmd),
                            stdout=subprocess.PIPE,
                            stdin=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    output = proc.communicate(script)
    return output[0]

def nodetool_multi(nodes, command):
    """Run a nodetool command simultaneously on each node specified"""
    class NodetoolThread(threading.Thread):
        """Run nodetool in a thread"""
        def __init__(self, cmd):
            threading.Thread.__init__(self)
            self.node = node
            self.cmd = cmd
        def run(self):
            self.output = nodetool(self.cmd)
    threads = {}
    for node in nodes:
        t = NodetoolThread("-h {node} {cmd}".format(node=node, cmd=command))
        t.start()
        threads[node] = t
    output = {}
    for node, t in threads.items():
        t.join()
        output[node] = t.output
    return output

def wait_for_compaction(nodes=None, check_interval=30, idle_confirmations=3,
                        compaction_throughput=16, allowed_connection_errors=10):
    """Wait for all currently scheduled compactions to finish on all (or just specified) nodes

    nodes - the nodes to check (None == all)
    check_interval - the time to wait between checks
    idle_confirmations - the number of checks that must show 0 compactions before we assume compactions are really done.
    compaction_throughput - the default compaction_throughput_mb_per_sec setting from the cassandra.yaml
    allowed_connection_errors - the number of consecutive connection errors allowed before we quit trying

    returns the duration all compactions took (margin of error: check_interval * idle_confirmations)
    """

    def compactionstats(nodes, check_interval):
        """Check for compactions via nodetool compactionstats"""
        pattern = re.compile("(^|\n)pending tasks: 0")
        nodes = set(nodes)
        while True:
            results = execute(common.multi_nodetool, cmd="compactionstats")
            for node, output in results.iteritems():
                if pattern.search(output.strip()):
                    nodes.discard(node)

            if len(nodes) == 0:
                break
            logger.info("Waiting for compactions (compactionstats) on nodes:")
            for node in nodes:
                logger.info("{node} - {output}".format(node=node, output=results[node]))
            time.sleep(check_interval)

        assert len(nodes) == 0, ("Compactions (compactionstats) should have finished, but they didn't"
                            " on nodes: {nodes}. output: {results}".format(
                                nodes=nodes, output=results))

    def tpstats(nodes, check_interval):
        """Check for compactions via nodetool tpstats"""
        stat_exists_pattern = re.compile("^CompactionExecutor", re.MULTILINE)
        no_compactions_pattern = re.compile("CompactionExecutor\W*0\W*0\W*[0-9]*\W*0", re.MULTILINE)

        nodes = set(nodes)
        while True:
            results = execute(common.multi_nodetool, cmd="tpstats")
            for node, output in results.iteritems():
                if stat_exists_pattern.search(output):
                    if no_compactions_pattern.search(output):
                        nodes.discard(node)
                else:
                    logger.warn("CompactionExecutor not listed in nodetool tpstats, can't check for compactions this way.")
                    return

            if len(nodes) == 0:
                break
            logger.info("Waiting for compactions (tpstats) on nodes: {nodes}".format(nodes=nodes))
            time.sleep(check_interval)

        assert len(nodes) == 0, ("Compactions (tpstats) should have finished, but they didn't"
                            " on nodes: {nodes}. output: {results}".format(
                                nodes=nodes, output=results))

    if nodes is None:
        nodes = set(common.fab.env.hosts)
    else:
        nodes = set(nodes)

    # Disable compaction throttling to speed things up:
    execute(common.multi_nodetool, cmd="setcompactionthroughput 0")

    # Perform checks multiple times to ensure compactions are really done:
    start = time.time()
    for i in range(idle_confirmations):
        compactionstats(nodes, check_interval)
        tpstats(nodes, check_interval)

    duration = time.time() - start

    # Re-enable compaction throttling:
    execute(common.multi_nodetool, cmd='setcompactionthroughput {compaction_throughput}'.format(**locals()))

    logger.info("Compactions finished on all nodes. Duration of checks: {duration}".format(**locals()))

    return duration

def set_device_read_ahead(read_ahead, devices=None):
    """Set device read ahead.
    
    If devices argument is None, use the 'block_devices' setting from the cluster config."""
    if devices is None:
        devices = config['block_devices']
    execute(common.set_device_read_ahead, read_ahead, devices)

def drop_page_cache():
    """Drop the page cache"""
    if not config.get('docker', False):
        bash(['sync', 'echo 3 > /proc/sys/vm/drop_caches'], user='root')

def setup_stress(stress_revision):
    stress_path = None

    try:
        git_id = sh.git('--git-dir={home}/fab/cassandra.git'
                        .format(home=HOME), 'rev-parse', stress_revision).strip()
    except sh.ErrorReturnCode:
        raise AssertionError('Invalid stress_revision: {}'.format(stress_revision))

    path = os.path.join(CASSANDRA_STRESS_PATH, git_id)
    if not os.path.exists(path):
        logger.info("Building cassandra-stress '{}' in '{}'.".format(stress_revision, path))
        os.makedirs(path)
        sh.tar(
            sh.git("--git-dir={home}/fab/cassandra.git".format(home=HOME), "archive", git_id),
            'x', '-C', path
        )
        antcmd('-Dbasedir={}'.format(path), '-f', '{}/build.xml'.format(path),
               'realclean', 'jar', _env={"JAVA_TOOL_OPTIONS": "-Dfile.encoding=UTF8",
                                         "JAVA_HOME": JAVA_HOME})

    stress_path = os.path.join(path, 'tools/bin/cassandra-stress')

    return stress_path


def stress(cmd, revision_tag, stats=None, stress_revision=None):
    """Run stress command and collect average statistics"""
    # Check for compatible stress commands. This doesn't yet have full
    # coverage of every option:
    # Make sure that if this is a read op, that the number of threads
    # was specified, otherwise stress defaults to doing multiple runs
    # which is not what we want:
    if cmd.strip().startswith("read") and 'threads' not in cmd:
        raise AssertionError('Stress read commands must specify #/threads when used with this tool.')

    stress_path = CASSANDRA_STRESS_DEFAULT
    if stress_revision:
        stress_path = setup_stress(stress_revision)

    temp_log = tempfile.mktemp()
    logger.info("Running stress from '{stress_path}' : {cmd}"
                .format(stress_path=stress_path, cmd=cmd))

    # Record the type of operation being performed:
    operation = cmd.strip().split(" ")[0]

    if stats is None:
        stats = {
            "id": str(uuid.uuid1()),
            "command": cmd,
            "intervals": [],
            "test": operation,
            "revision": revision_tag,
            "date": datetime.datetime.now().isoformat(),
            "stress_revision": stress_revision
        }

    # Run stress:
    # Subprocess communicate() blocks, preventing us from seeing any
    # realtime output, so pipe the output to a file as a workaround:
    proc = subprocess.Popen('JAVA_HOME={JAVA_HOME} {CASSANDRA_STRESS} {cmd} | tee {temp_log}'
                            .format(JAVA_HOME=JAVA_HOME,
                                    CASSANDRA_STRESS=stress_path,
                                    cmd=cmd, temp_log=temp_log), shell=True)
    proc.wait()
    log = open(temp_log)
    collecting_aggregates = False
    collecting_values = False
    
    # Regex for trunk cassandra-stress
    start_of_intervals_re = re.compile('type,.*total ops,.*op/s,.*pk/s')
    
    for line in log:
        if line.startswith("Results:"):
            collecting_aggregates = True
            continue
        if not collecting_aggregates:
            if start_of_intervals_re.match(line):
                collecting_values = True
                continue
            if collecting_values:
                line_parts = [l.strip() for l in line.split(',')]
                # Only capture total metrics for now
                if line_parts[0] == 'total':
                    try:
                        stats['intervals'].append([float(x) for x in line_parts[1:]])
                    except:
                        pass
                continue
            continue
        if line.startswith("END") or line == "":
            continue
        # Collect aggregates:
        stat, value  = line.split(":", 1)
        stats[stat.strip()] = value.strip()
    log.close()
    os.remove(temp_log)
    return stats


def retrieve_logs(local_directory):
    """Retrieve each node's logs to the given local directory."""
    execute(common.copy_logs, local_directory=local_directory)

def retrieve_fincore_logs(local_directory):
    """Retrieve each node's fincore logs to the given local directory."""
    execute(common.copy_fincore_logs, local_directory=local_directory)

def start_fincore_capture(interval=10):
    """Start linux-fincore monitoring of Cassandra data files on each node"""
    execute(common.start_fincore_capture, interval=interval)

def stop_fincore_capture():
    """Stop linux-fincore monitoring"""
    execute(common.stop_fincore_capture)

def log_add_data(file, data):
    """Merge the dictionary data into the json log file root."""
    with open(file) as f:
        log = f.read()
        log = json.loads(log)
        log.update(data)
        log = json.dumps(log, sort_keys=True, indent=4, separators=(', ', ': '))
    with open(file, 'w') as f:
        f.write(log)

def log_set_title(file, title, subtitle=''):
    log_add_data(file, {'title': title, 'subtitle': subtitle})

def log_stats(stats, memo=None, file='stats.json'):
    """Log results"""
    # TODO: this should go back into a cassandra store for long term
    # keeping
    if not os.path.exists(file) or os.path.getsize(file) == 0:
        with open(file, 'w') as f:
            f.write(json.dumps({'title': 'Title goes here', 'stats':[]}))

    with open(file) as f:
        log = f.read()
        log = json.loads(log)
        if memo:
            stats.update({'memo': memo})
        log['stats'].append(stats)
        log = json.dumps(log, sort_keys=True, indent=4, separators=(', ', ': '))

    with open(file, 'w') as f:
        f.write(log)

