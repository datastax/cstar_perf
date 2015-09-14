import os
import requests
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

def get_cassandra_path():
    return os.path.join(get_dse_path(), 'resources/cassandra/')

def get_bin_path():
    return os.path.join(get_dse_path(), 'bin')

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
    cmd = 'JAVA_HOME={java_home} nohup ~/fab/dse/bin/dse cassandra'.format(
        java_home=config['java_home'])
    fab.run(cmd)

def stop(clean):
    fab.run('jps | grep DseDaemon | cut -d" " -f1 | xargs kill -9', quiet=True)

def is_running():
    jps = fab.run('jps | grep DseDaemon"', quiet=True)
    return True if jps.return_code == 0 else False
