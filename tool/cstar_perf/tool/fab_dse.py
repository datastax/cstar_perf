import logging
import os
import re
import uuid
from urlparse import urljoin
from StringIO import StringIO

import yaml
from fabric import api as fab
from fabric.state import env

from util import download_file, download_file_contents, digest_file

logging.basicConfig()
logger = logging.getLogger('dse')
logger.setLevel(logging.INFO)


# I don't like global ...
global config
global dse_builds, dse_cache, dse_tarball

DSE_NODE_TYPE_TO_STARTUP_PARAM = {'spark': '-k',
                                  'hadoop': '-t',
                                  'cassandra': '',
                                  'search': '-s',
                                  'spark-hadoop': '-k -t',
                                  'search-analytics': '-s -k'}

name = 'dse'


def setup(cfg):
    "Local setup for dse"

    global config, dse_builds, dse_cache, dse_tarball

    config = cfg

    if 'dse_url' not in config:
        raise ValueError("dse_url is missing from cluster_config.json.")

    # Create dse_cache_dir
    dse_builds = os.path.expanduser("~/fab/dse_builds")
    dse_cache = os.path.join(dse_builds, '_cache')
    if not os.path.exists(dse_cache):
        os.makedirs(dse_cache)

    download_tarball = True

    revision = config['revision']
    logger.info('Using DSE revision: {rev}'.format(rev=revision))
    if revision.startswith('bdp/'):
        download_tarball = False
        oauth_token = config.get('dse_source_build_oauth_token', None)
        if not config.get('dse_source_build_oauth_token', None):
            raise ValueError('dse_source_build_oauth_token for checking out a DSE branch is missing from cluster_config.json.')

        if not config.get('dse_source_build_artifactory_username', None):
            raise ValueError('dse_source_build_artifactory_username for building a DSE branch is missing from cluster_config.json.')

        if not config.get('dse_source_build_artifactory_password', None):
            raise ValueError('dse_source_build_artifactory_password for building a DSE branch is missing from cluster_config.json.')

        if not config.get('dse_source_build_artifactory_url', None):
            raise ValueError('dse_source_build_artifactory_url for building a DSE branch is missing from cluster_config.json.')

        _checkout_dse_branch_and_build_tarball_from_source(branch=revision[4:])

    if download_tarball:
        dse_tarball = "dse-{revision}-bin.tar.gz".format(revision=revision)
        download_binaries()


def download_binaries():
    "Parse config and download dse binaries (local)"

    # TODO since this is done locally on the cperf tool server, is there any possible concurrency
    # issue .. Or maybe we should simply keep a cache on each host? (Comment to remove)
    filename = os.path.join(dse_cache, dse_tarball)

    dse_url = config['dse_url']
    username = config['dse_username'] if 'dse_username' in config else None
    password = config['dse_password'] if 'dse_password' in config else None
    url = urljoin(dse_url, dse_tarball)

    # Fetch the SHA of the tarball: download_file_contents returns the request.text of the url.
    # the sha file has the format '874c11f7634974fb41006d30199b55b59fd124db ?./dse-5.0.0-bin.tar.gz'
    # so we split on the space and then check that the sha hexidecimal is 40 characters
    correct_sha = download_file_contents(url+'.sha', username, password).split(" ")[0]
    assert(len(correct_sha) == 40), 'Failed to download sha file: {}'.format(correct_sha)

    if os.path.exists(filename):
        logger.info("Already in cache: {}".format(filename))
        real_sha = digest_file(filename)
        if real_sha != correct_sha:
            logger.info("Invalid SHA for '{}'. It will be removed".format(filename))
            os.remove(filename)
        else:
            return

    # Fetch the tarball:
    request = download_file(url, filename, username, password)
    real_sha = digest_file(filename)
    # Verify the SHA of the tarball:
    if real_sha != correct_sha:
        raise AssertionError(
            ('SHA of DSE tarball was not verified. should have been: '
             '{correct_sha} but saw {real_sha}').format(correct_sha=correct_sha, real_sha=real_sha))


def get_dse_path():
    return os.path.expanduser("~/fab/dse")


def get_dse_conf_path():
    return os.path.join(get_dse_path(), 'resources', 'dse', 'conf')

def get_cassandra_path():
    return os.path.join(get_dse_path(), 'resources/cassandra/')

def get_bin_path():
    dse_home = 'DSE_HOME={dse_path}'.format(dse_path=get_dse_path())
    return os.path.join('{dse_home} {dse_path}'.format(dse_home=dse_home, dse_path=get_dse_path()), 'bin')

def bootstrap(config):
    filename = os.path.join(dse_cache, dse_tarball)
    dest = os.path.join(dse_builds, dse_tarball)
    # remove build folder if it exists
    fab.run('rm -rf {}'.format(os.path.join(dse_builds, dse_tarball.replace('-bin.tar.gz', ''))))

    # Upload the binaries
    fab.run('mkdir -p {dse_builds}'.format(dse_builds=dse_builds))
    fab.put(filename, dest)

    # Extract the binaries
    fab.run('tar -C {dse_builds} -xf {dest}'.format(dse_builds=dse_builds, dest=dest))

    # Symlink current build to ~/fab/dse
    fab.run('ln -sf {} ~/fab/dse'.format(os.path.join(dse_builds, dse_tarball.replace('-bin.tar.gz', ''))))

    return config['revision']

def start(config):
    _configure_spark_env(config)

    dse_node_type = config.get('dse_node_type', 'cassandra')
    fab.puts("Starting DSE - Node Type: {node_type}".format(node_type=dse_node_type))
    dse_home = 'DSE_HOME={dse_path}'.format(dse_path=get_dse_path())
    cmd = 'JAVA_HOME={java_home} {dse_home} nohup {dse_path}/bin/dse cassandra {node_type}'.format(
        java_home=config['java_home'], dse_home=dse_home, dse_path=get_dse_path(),
        node_type=DSE_NODE_TYPE_TO_STARTUP_PARAM.get(dse_node_type, ''))
    fab.run(cmd)

def stop(clean, config):
    fab.run('jps | grep DseDaemon | cut -d" " -f1 | xargs kill -9', quiet=True)

def is_running():
    jps = fab.run('jps | grep DseDaemon"', quiet=True)
    return True if jps.return_code == 0 else False


def _download_jython_if_necessary():
    # Get Jython helper :
    jython_status = fab.run('test -f ~/fab/jython.jar', quiet=True)
    if jython_status.return_code > 0:
        fab.run("wget http://search.maven.org/remotecontent?filepath=org/python/jython-standalone/2.7-b1/jython-standalone-2.7-b1.jar -O ~/fab/jython.jar")


def _get_config_options(config, config_class='org.apache.cassandra.config.Config'):
    _download_jython_if_necessary()

    dse_lib_folder = os.path.join('{dse}'.format(dse=get_dse_path().replace('~', '$HOME')), 'lib', '*')
    cass_lib_folder = os.path.join('{cass}'.format(cass=get_cassandra_path().replace('~', '$HOME')), 'lib', '*')

    classpath = ":".join([dse_lib_folder, cass_lib_folder, "$HOME/fab/jython.jar"])
    cmd = '{java_home}/bin/java -cp "{classpath}" org.python.util.jython -c "import {config_class} as Config; print dict(Config.__dict__).keys()"'.format(java_home=config['java_home'], **locals())

    out = fab.run(cmd, combine_stderr=False)
    if out.failed:
        fab.abort('Failed to run Jython Config parser : ' + out.stderr)
    opts = yaml.load(out)
    p = re.compile("^[a-z][^A-Z]*$")
    return [o for o in opts if p.match(o)]


def get_cassandra_config_options(config):
    """Parse Cassandra's Config class to get all possible config values.

    Unfortunately, some are hidden from the default cassandra.yaml file, so this appears the only way to do this."""
    return _get_config_options(config=config, config_class='org.apache.cassandra.config.Config')


def get_dse_config_options(config):
    """
    Parse DSE Config class to get all possible dse.yaml config values

    """
    return _get_config_options(config=config, config_class='com.datastax.bdp.config.Config')

def _checkout_dse_branch_and_build_tarball_from_source(branch):
    global dse_cache, dse_tarball, config

    java_home = config['java_home']
    oauth_token = config.get('dse_source_build_oauth_token')
    bdp_git = '~/fab/bdp.git'

    _setup_maven_authentication(config)
    _setup_gradle_authentication(config)

    fab.local('rm -rf {bdp_git}'.format(bdp_git=bdp_git))
    fab.local('mkdir -p {bdp_git}'.format(bdp_git=bdp_git))
    fab.local('git clone -b {branch} --single-branch https://{oauth_token}@github.com/riptano/bdp.git {bdp_git}'.format(
        branch=branch, oauth_token=oauth_token, bdp_git=bdp_git))

    # build the tarball from source
    env.ok_ret_codes = [0, 1]
    gradle_file_exists = fab.local('[ -e  {bdp_git}/build.gradle ]'.format(bdp_git=bdp_git))
    env.ok_ret_codes = [0]

    if gradle_file_exists.return_code == 0:
        # run the gradle build
        fab.local('export TERM=dumb; cd {bdp_git}; ./gradlew distTar -PbuildType={branch}'.format(bdp_git=bdp_git, branch=branch))
    else:
        # run the ant build
        fab.local(
            'cd {bdp_git}; JAVA_HOME={java_home} ANT_HOME=$HOME/fab/ant/ $HOME/fab/ant/bin/ant -Dversion={branch} -Dcompile.native=true release'.format(
                branch=branch, java_home=java_home, bdp_git=bdp_git))

    # we need to expand the tarball name because the name will look as following: dse-{version}-{branch}-bin.tar.gz
    # example: dse-4.8.3-4.8-dev-bin.tar.gz
    path_name = fab.local('readlink -e {bdp_git}/build/dse-*{branch}-bin.tar.gz'.format(bdp_git=bdp_git, branch=branch),
                          capture=True)
    dse_tarball = os.path.basename(path_name)

    logger.info('Created tarball from source: {tarball}'.format(tarball=dse_tarball))
    fab.local('cp {bdp_git}/build/{tarball} {dse_cache}'.format(bdp_git=bdp_git, dse_cache=dse_cache, tarball=dse_tarball))

    # remove the maven & gradle settings after the tarball got created
    fab.local('rm -rf ~/.m2/settings.xml')
    fab.local('rm -rf ~/.gradle')


def _setup_maven_authentication(config):
    dse_source_build_artifactory_username = config.get('dse_source_build_artifactory_username')
    dse_source_build_artifactory_password = config.get('dse_source_build_artifactory_password')
    dse_source_build_artifactory_url = config.get('dse_source_build_artifactory_url')

    maven_settings = "<settings><mirrors><mirror><id>artifactory</id><name>DataStax Maven repository</name>" \
                     "<url>{url}</url>" \
                     "<mirrorOf>central,java.net2,xerial,datanucleus,apache,datastax-public-snapshot,datastax-public-release,datastax-deps,datastax-release,datastax-snapshot</mirrorOf>" \
                     "</mirror></mirrors><servers><server><id>artifactory</id><username>{username}</username>" \
                     "<password>{password}</password></server></servers></settings>" \
        .format(username=dse_source_build_artifactory_username, password=dse_source_build_artifactory_password,
                url=dse_source_build_artifactory_url)

    fab.local('rm -rf ~/.m2/settings.xml')
    fab.local('mkdir -p ~/.m2')
    fab.local('echo "{maven_settings}" > ~/.m2/settings.xml'.format(maven_settings=maven_settings))


def _setup_gradle_authentication(config):
    dse_source_build_artifactory_username = config.get('dse_source_build_artifactory_username')
    dse_source_build_artifactory_password = config.get('dse_source_build_artifactory_password')
    dse_source_build_artifactory_url = config.get('dse_source_build_artifactory_url')

    gradle_settings = """
allprojects {{
    repositories {{
        maven {{
            url = "\\"{url}\\""
            credentials {{
                username '{username}'
                password '{password}'
            }}
        }}
    }}
}}
    """.format(username=dse_source_build_artifactory_username, password=dse_source_build_artifactory_password,
               url=dse_source_build_artifactory_url)

    fab.local('rm -rf ~/.gradle')
    fab.local('mkdir -p ~/.gradle/init.d')
    fab.local('echo "{gradle_settings}" > ~/.gradle/init.d/nexus.gradle'.format(gradle_settings=gradle_settings))


def _configure_spark_env(config):
    # Place spark environment file on hosts:
    spark_env = config.get('spark_env', '')

    if isinstance(spark_env, list) or isinstance(spark_env, tuple):
        spark_env = "\n".join(spark_env)
    spark_env += "\n"

    spark_env_script = "spark-{name}.sh".format(name=uuid.uuid1())
    spark_env_file = StringIO(spark_env)
    fab.run('mkdir -p ~/fab/scripts')
    fab.put(spark_env_file, '~/fab/scripts/{spark_env_script}'.format(spark_env_script=spark_env_script))

    fab.puts('spark-env is: {}'.format(spark_env))
    if len(spark_env_script) > 0:
        spark_env_path = os.path.join(get_dse_path(), 'resources', 'spark', 'conf', 'spark-env.sh')
        fab.run('cat ~/fab/scripts/{spark_env_script} >> {spark_env_path}'.format(spark_env_script=spark_env_script,
                                                                                  spark_env_path=spark_env_path))
