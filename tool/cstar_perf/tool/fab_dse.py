import os
import requests
import yaml
import re
from urlparse import urljoin
from fabric import api as fab
from util import download_file, download_file_contents, digest_file

# I don't like global ...
global config
global dse_builds, dse_cache, dse_tarball

name = 'dse'

def setup(cfg):
    "Local setup for dse"

    global config, dse_builds, dse_cache, dse_tarball

    config = cfg

    if 'dse_url' not in  config:
        raise ValueError("dse_url is missing from cluster_config.json.")

    dse_tarball = "dse-{}-bin.tar.gz".format(config['revision'])

    # Create dse_cache_dir
    dse_builds = os.path.expanduser("~/fab/dse_builds")
    dse_cache = os.path.join(dse_builds, '_cache')
    if not os.path.exists(dse_cache):
        os.makedirs(dse_cache)

    if config["product"] == 'dse':
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

    # Fetch the SHA of the tarball:
    correct_sha = download_file_contents(url+'.sha', username, password).split(" ")[0]
    assert(len(correct_sha) == 64, 'Failed to download sha file: {}'.format(correct_sha))

    if os.path.exists(filename):
        print("Already in cache: {}".format(filename))
        real_sha = digest_file(filename)
        if real_sha != correct_sha:
            print("Invalid SHA for '{}'. It will be removed".format(filename))
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
    return "~/fab/dse"


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

    # Upload the binaries
    fab.run('mkdir -p {dse_builds}'.format(dse_builds=dse_builds))
    fab.put(filename, dest)

    # Extract the binaries
    fab.run('tar -C {dse_builds} -xf {dest}'.format(dse_builds=dse_builds, dest=dest))

    # Symlink current build to ~/fab/dse
    fab.run('ln -sf {} ~/fab/dse'.format(os.path.join(dse_builds, dse_tarball.replace('-bin.tar.gz', ''))))

    return config['revision']

def start(config):
    fab.puts("Starting DSE Cassandra..")
    dse_home = 'DSE_HOME={dse_path}'.format(dse_path=get_dse_path())
    cmd = 'JAVA_HOME={java_home} {dse_home} nohup {dse_path}/bin/dse cassandra'.format(
        java_home=config['java_home'], dse_home=dse_home, dse_path=get_dse_path())
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


def get_cassandra_config_options(config):
    """Parse Cassandra's Config class to get all possible config values.

    Unfortunately, some are hidden from the default cassandra.yaml file, so this appears the only way to do this."""
    _download_jython_if_necessary()

    dse_lib_folder = os.path.join('{dse}'.format(dse=get_dse_path().replace('~', '$HOME')), 'lib', '*')
    cass_lib_folder = os.path.join('{cass}'.format(cass=get_cassandra_path().replace('~', '$HOME')), 'lib', '*')

    classpath = ":".join([dse_lib_folder, cass_lib_folder, "$HOME/fab/jython.jar"])
    cmd = '{java_home}/bin/java -cp "{classpath}" org.python.util.jython -c "import org.apache.cassandra.config.Config as Config; print dict(Config.__dict__).keys()"'.format(java_home=config['java_home'], **locals())

    out = fab.run(cmd, combine_stderr=False)
    if out.failed:
        fab.abort('Failed to run Jython Config parser : ' + out.stderr)
    opts = yaml.load(out)
    p = re.compile("^[a-z][^A-Z]*$")
    return [o for o in opts if p.match(o)]


def get_dse_config_options(config):
    """
    Parse DSE Config class to get all possible dse.yaml config values

    """
    _download_jython_if_necessary()

    dse_lib_folder = os.path.join('{dse}'.format(dse=get_dse_path().replace('~', '$HOME')), 'lib', '*')
    cass_lib_folder = os.path.join('{cass}'.format(cass=get_cassandra_path().replace('~', '$HOME')), 'lib', '*')

    classpath = ":".join([dse_lib_folder, cass_lib_folder, "$HOME/fab/jython.jar"])
    cmd = '{java_home}/bin/java -cp "{classpath}" org.python.util.jython -c "import com.datastax.bdp.config.Config as Config; print dict(Config.__dict__).keys()"'.format(java_home=config['java_home'], **locals())

    out = fab.run(cmd, combine_stderr=False)
    if out.failed:
        fab.abort('Failed to run Jython Config parser : ' + out.stderr)
    opts = yaml.load(out)
    p = re.compile("^[a-z][^A-Z]*$")
    return [o for o in opts if p.match(o)]
