"""
Bootstrap Cassandra onto a cluster and benchmark stress.
"""

import subprocess
import tempfile
import os
import time
import datetime
from StringIO import StringIO
from collections import namedtuple
from pprint import pprint
import uuid
import re
import json
import socket
import getpass
import logging
import itertools
import shutil
import distutils.util

from fabric.tasks import execute
import fabric.api as fab
import yaml

import sh
import shlex


# Import the default config first:r
import fab_common as common
import fab_dse as dse
import fab_cassandra as cstar
import fab_flamegraph as flamegraph
import fab_profiler as profiler

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

CSTAR_PERF_LOGS_DIR = os.path.join(os.path.expanduser('~'), '.cstar_perf', 'logs')

antcmd = sh.Command(os.path.join(HOME, 'fab/ant/bin/ant'))

global nodetool_path, cqlsh_path

def set_nodetool_path(path):
    global nodetool_path
    nodetool_path = path

def set_cqlsh_path(path):
    global cqlsh_path
    if path.startswith('DSE_HOME'):
        path = path[path.find(' ') + 1:]
    cqlsh_path = path

def get_localhost():
    ip = socket.gethostname().split(".")[0]
    return (ip, getpass.getuser() + "@" + ip)

def get_all_hosts(env):
    # the local host will not be added to the cluster unless
    # it has a corresponding entry in the cluster config:
    hosts = list(env['hosts'])
    localhost_ip, localhost_entry = get_localhost()
    if localhost_ip not in [host.split(".")[0] for host in hosts]:
        # Use the local username for this host, as it may be different
        # than the cluster defined 'user' parameter:
        hosts += [localhost_entry]
    return hosts


def _parse_yaml(yaml_file):
    if isinstance(yaml_file, basestring):
        yaml_file = yaml.load(yaml_file)
    if yaml_file is None:
        yaml_file = {}
    if type(yaml_file) is not dict:
        raise ValueError('Invalid yaml, was expecting a dictionary: {cass_yaml}'.format(cass_yaml=yaml_file))
    return yaml_file


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
        common.config['yaml'] = _parse_yaml(cass_yaml)
    if cfg.has_key('dse_yaml'):
        dse_yaml = cfg['dse_yaml']
        common.config['dse_yaml'] = _parse_yaml(dse_yaml)
    if cfg.has_key('options'):
        if cfg['options'] is not None:
            common.config.update(cfg['options'])
            del common.config['options']
        # Rerun setup now that additional options have been added:
        common.setup(common.config)
            
    logger.info("### Config: ###")
    pprint(common.config)

    # leave_data settting can be set in the revision
    # configuration, or manually in the call to this function.
    # Either is fine, but they shouldn't conflict. If they do,
    # ValueError is raised.
    if leave_data == True and cfg.get('leave_data', None) == False:
        raise ValueError('setting for leave_data conflicts in job config and bootstrap() call')
    else:
        leave_data = bool(distutils.util.strtobool(str(cfg.get('leave_data', leave_data))))

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

    replace_existing_dse_install = bool(distutils.util.strtobool(str(cfg.get('replace_existing_dse_install', 'True'))))
    # dse setup and binaries download (local)
    if product == dse and replace_existing_dse_install:
        dse.setup(common.config)

    set_nodetool_path(os.path.join(product.get_bin_path(), 'nodetool'))
    set_cqlsh_path(os.path.join(product.get_bin_path(), 'cqlsh'))

    # Bootstrap C* onto the cluster nodes, as well as the localhost,
    # so we have access to nodetool, stress etc
    hosts = get_all_hosts(common.fab.env)
    if not cfg.get('revision_override'):
        with common.fab.settings(hosts=hosts):
            git_ids = execute(common.bootstrap, git_fetch=git_fetch, replace_existing_dse_install=replace_existing_dse_install)
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
    time.sleep(15)
    is_running = True
    with fab.settings(abort_exception=SystemExit):
        try:
            execute(common.ensure_running, hosts=[common.config['seeds'][0]])
            time.sleep(30)
        except SystemExit:
            is_running = False

    if not is_running:
        try:
            retrieve_logs_and_create_tarball(job_id=_extract_job_id())
        except Exception as e:
            logger.warn(e)
            pass
        fab.abort('Cassandra is not up!')

    logger.info("Started {product} on {n} nodes with git SHAs: {git_ids}".format(
        product=product.name, n=len(common.fab.env['hosts']), git_ids=git_ids))
    time.sleep(30)
    return git_ids


def _extract_job_id():
    # this will have a string looking as following: /home/cstar/.cstar_perf/jobs/<jobid>/stats.<jobid>.json
    stats_log = common.config.get('log')
    # will give us: <jobid>
    return stats_log.split(os.path.sep)[-2]


def retrieve_logs_and_create_tarball(job_id):
    log_dir = os.path.join(CSTAR_PERF_LOGS_DIR, job_id)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    retrieve_logs(log_dir)
    # Tar them for archiving:
    subprocess.Popen(shlex.split('tar cfvz {id}.tar.gz {id}'.format(id=job_id)), cwd=CSTAR_PERF_LOGS_DIR).communicate()
    shutil.rmtree(log_dir)


def restart():
    execute(common.stop)
    execute(common.ensure_stopped)
    execute(common.start)
    execute(common.ensure_running)

def teardown(destroy=False, leave_data=False, kill_delay=0):
    if destroy:
        execute(common.destroy, leave_data=leave_data, kill_delay=kill_delay)
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
        return execute(common.bash, script)


def cqlsh(script, node):
    """Run a cqlsh script on a node"""
    global cqlsh_path
    script = script.replace('\n', ' ')
    cmd = '{cqlsh_path} --no-color {host} -e "{script}"'.format(cqlsh_path=cqlsh_path, host=node, script=script)

    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
        return execute(fab.run, cmd)[node]


def dse_cmd(node, options):
    cmd = "JAVA_HOME={java_home} {dse_cmd} {options}".format(java_home=JAVA_HOME,
                                                             dse_cmd=os.path.join(dse.get_bin_path(), 'dse'),
                                                             options=options)
    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node, warn_only=True):
        return execute(fab.run, cmd)[node]


def dsetool_cmd(nodes, options):
    """Run a dsetool command simultaneously on each node specified"""
    cmd = 'JAVA_HOME={java_home} {dsetool_cmd} {options}'.format(java_home=JAVA_HOME,
                                                                 dsetool_cmd=os.path.join(dse.get_bin_path(),
                                                                                          'dsetool'), options=options)
    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=nodes, warn_only=True):
        return execute(fab.run, cmd)


def spark_cassandra_stress(script, node):
    download_and_build_spark_cassandra_stress(node)
    dse_bin = os.path.join(dse.get_dse_path(), 'bin')
    cmd = "cd {spark_cass_stress_path}; PATH=$PATH:{dse_bin} JAVA_HOME={JAVA_HOME} DSE_HOME={dse_home} ./run.sh dse {script}".format(JAVA_HOME=JAVA_HOME,
                                                                                            spark_cass_stress_path=get_spark_cassandra_stress_path(),
                                                                                            script=script, dse_bin=dse_bin, dse_home=dse.get_dse_path())
    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
        execute(fab.sudo, 'rm -rf /var/lib/spark')
        execute(fab.sudo, 'mkdir -p /var/lib/spark')
        execute(fab.sudo, 'chmod -R 777 /var/lib/spark')
        return execute(fab.run, cmd)


def _is_filename(possible_filename, node):
    # Check for some obvious things that aren't in the solr_stress filenames before doing a remote execute
    if not possible_filename or any([char in possible_filename for char in ' <>!']):
        return False
    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
        results = execute(fab.run, '[ -f {} ]; echo $?'.format(possible_filename), warn_only=True)
    return results[node] == '0'


def _write_config_file(operation_dir, possible_file, config_def, file_type, node):
    tmp_filename = os.path.join(operation_dir, file_type)
    if _is_filename(possible_file, node):
        with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
            execute(fab.run, 'cp {src} {dst}'.format(src=possible_file, dst=tmp_filename))
    else:
        conf_file = StringIO()
        conf_file.write(config_def)
        conf_file.seek(0)
        with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
            execute(fab.put, conf_file, tmp_filename)
    return tmp_filename


def _get_configs(operation_dir, schema_path, node, **configs):
    for config_type, config_definition in configs.items():
        possible_file = os.path.join(schema_path, config_definition)
        configs[config_type] = _write_config_file(operation_dir, possible_file, config_definition, config_type, node)
    return configs


def _remove_operation_dir(operation_dir, node):
    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
        execute(fab.run, 'rm -r {}'.format(operation_dir))


def _retrieve_solr_logs(operation_num, configs, local_dir):
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    for filename in configs.values():
        local_path = os.path.join(local_dir, 'operation{}_{}'.format(operation_num, os.path.basename(filename)))
        execute(common.copy_artifact, local_path=local_path, remote_path=filename)


def _create_operation_working_directory(operation_id, node):
    operation_dir = '/tmp/{}'.format(operation_id)
    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
        execute(fab.run, 'mkdir -p {}'.format(operation_dir))
    return operation_dir


def solr_create_schema(operation_id, operation_num, schema, solrconfig, cql, core, node):
    operation_dir = _create_operation_working_directory(operation_id, node)
    schema_path = os.path.join(dse.get_dse_path(), 'demos', 'solr_stress', 'resources', 'schema')
    configs = _get_configs(operation_dir, schema_path, node, schema=schema, solrconfig=solrconfig, cql=cql)

    cmd = 'cd {schema_path}; ./create-schema.sh -x {schema} -r {solrconfig} -t {cql} -k {core}'.format(
        schema_path=schema_path,
        schema=configs['schema'],
        solrconfig=configs['solrconfig'],
        cql=configs['cql'],
        core=core,
    )

    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
        result = execute(fab.run, cmd)

    local_dir = os.path.join(os.path.expanduser('~'), '.cstar_perf/operation_artifacts')
    _retrieve_solr_logs(operation_num, configs, local_dir)
    _remove_operation_dir(operation_dir, node)

    return result


def solr_run_benchmark(operation_id, operation_num, testdata, args, node):
    operation_dir = _create_operation_working_directory(operation_id, node)
    run_benchmark_path = os.path.join(dse.get_dse_path(), 'demos', 'solr_stress')
    resources_path = os.path.join(run_benchmark_path, 'resources')
    configs = _get_configs(operation_dir, resources_path, node, testdata=testdata)

    cmd = 'cd {path}; ./run-benchmark.sh --test-data {testdata} {args}'.format(
        path=run_benchmark_path,
        testdata=configs['testdata'],
        args=args)

    with common.fab.settings(fab.show('warnings', 'running', 'stdout', 'stderr'), hosts=node):
        result = execute(fab.run, cmd)

    local_dir = os.path.join(os.path.expanduser('~'), '.cstar_perf/operation_artifacts')
    _retrieve_solr_logs(operation_num, configs, local_dir)
    execute(common.copy_artifact, local_path=local_dir, remote_path=os.path.join(run_benchmark_path, "exceptions.log"))
    _remove_operation_dir(operation_dir, node)
    return result


def get_spark_cassandra_stress_path():
    return os.path.expanduser("~/fab/spark-cassandra-stress")


def download_and_build_spark_cassandra_stress(node):
    dse_home = 'DSE_HOME={dse_path}'.format(dse_path=dse.get_dse_path())
    dse_resources = 'DSE_RESOURCES={dse_resources_path}'.format(dse_resources_path=os.path.join(dse.get_dse_path(), 'resources'))
    build_command = './gradlew jar -Pagainst=dse;'

    with common.fab.settings(hosts=node):
        execute(fab.run, 'rm -rf {spark_cass_stress_path}'.format(spark_cass_stress_path=get_spark_cassandra_stress_path()))
        execute(fab.run, 'git clone -b master --single-branch https://github.com/datastax/spark-cassandra-stress.git {spark_cass_stress_path}'
            .format(spark_cass_stress_path=get_spark_cassandra_stress_path()))
        return execute(fab.run, 'cd {spark_cass_stress_path}; TERM=dumb {dse_home} {dse_resources} {build_cmd}'
            .format(spark_cass_stress_path=get_spark_cassandra_stress_path(), dse_home=dse_home, dse_resources=dse_resources, build_cmd=build_command))


def nodetool_multi(nodes, command):
    """Run a nodetool command simultaneously on each node specified"""
    with common.fab.settings(hosts=nodes):
        return execute(common.multi_nodetool, command)


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
        consecutive_connection_errors = 0
        pattern = re.compile("(^|\n)pending tasks: 0")
        failure_pattern = re.compile("ConnectException")
        nodes = set(nodes)
        while True:
            results = execute(common.multi_nodetool, cmd="compactionstats")
            for node, output in results.iteritems():
                if pattern.search(output.strip()):
                    nodes.discard(node)
                elif failure_pattern.search(output.strip()):
                    consecutive_connection_errors += 1

            if consecutive_connection_errors > allowed_connection_errors:
                raise NodetoolException(
                    "Failed to connect via nodetool {consecutive_connection_errors} times in a row.".format(
                        consecutive_connection_errors=consecutive_connection_errors))

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
        consecutive_connection_errors = 0
        stat_exists_pattern = re.compile("^CompactionExecutor", re.MULTILINE)
        no_compactions_pattern = re.compile("CompactionExecutor\W*0\W*0\W*[0-9]*\W*0", re.MULTILINE)
        failure_pattern = re.compile("ConnectException")

        nodes = set(nodes)
        while True:
            results = execute(common.multi_nodetool, cmd="tpstats")
            for node, output in results.iteritems():
                if stat_exists_pattern.search(output):
                    if no_compactions_pattern.search(output):
                        nodes.discard(node)
                elif failure_pattern.search(output.strip()):
                    consecutive_connection_errors += 1
                else:
                    logger.warn("CompactionExecutor not listed in nodetool tpstats, can't check for compactions this way.")
                    return

            if consecutive_connection_errors > allowed_connection_errors:
                raise NodetoolException(
                    "Failed to connect via nodetool {consecutive_connection_errors} times in a row.".format(
                        consecutive_connection_errors=consecutive_connection_errors))

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

def clean_stress():
    # Clean all stress builds
    stress_builds = [b for b in os.listdir(CASSANDRA_STRESS_PATH)]
    for stress_build in stress_builds:
        path = os.path.join(CASSANDRA_STRESS_PATH, stress_build)
        logger.info("Removing stress build '{}'".format(path))
        if os.path.islink(path):
            os.unlink(path)
        else:
            shutil.rmtree(path)

def build_stress(stress_revision, name=None):
    # Build a stress revision

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

    name = name if name else stress_revision
    return {name: git_id}

def setup_stress(stress_revisions=[]):
    revisions = {}

    # first, build the default revision
    default_stress_revision = config.get('stress_revision', 'apache/trunk')
    revisions.update(build_stress(default_stress_revision, name='default'))

    for stress_revision in stress_revisions:
        revisions.update(build_stress(stress_revision))

    return revisions


def stress(cmd, revision_tag, stress_sha, stats=None):
    """Run stress command and collect average statistics"""
    # Check for compatible stress commands. This doesn't yet have full
    # coverage of every option:
    # Make sure that if this is a read op, that the number of threads
    # was specified, otherwise stress defaults to doing multiple runs
    # which is not what we want:
    if cmd.strip().startswith("read") and 'threads' not in cmd:
        raise AssertionError('Stress read commands must specify #/threads when used with this tool.')

    stress_path = os.path.join(CASSANDRA_STRESS_PATH, stress_sha, 'tools/bin/cassandra-stress')

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
            "stress_revision": stress_sha
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
    start_of_intervals_re = re.compile('type.*total ops,.*op/s,.*pk/s')
    for line in log:
        line = line.strip()
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
        if line.startswith("END") or line.strip() == "":
            continue
        # Collect aggregates:
        try:
            stat, value = line.split(":", 1)
            stats[stat.strip().lower()] = value.strip()
        except ValueError:
            logger.info("Unable to parse aggregate line: '{}'".format(line))
    log.close()
    os.remove(temp_log)
    return stats


def retrieve_logs(local_directory):
    """Retrieve each node's logs to the given local directory."""
    execute(common.copy_logs, local_directory=local_directory)

def retrieve_fincore_logs(local_directory):
    """Retrieve each node's fincore logs to the given local directory."""
    execute(common.copy_fincore_logs, local_directory=local_directory)

def retrieve_flamegraph(local_directory, rev_num):
    """Retrieve each node's flamegraph data and svg to the given local directory."""
    execute(flamegraph.copy_flamegraph, local_directory=local_directory, rev_num=rev_num)

def retrieve_yourkit(local_directory, rev_num):
    """Retrieve each node's yourkit data to the given local directory."""
    execute(profiler.copy_yourkit, local_directory=local_directory, rev_num=rev_num)

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
