import os
import requests
from urlparse import urljoin
from fabric import api as fab
from util import download_file

# I don't like global ...
global config
global dse_cache, dse_binaries

name = 'dse'

def setup(cfg):
    "Local setup for dse"

    global config, dse_cache, dse_binaries

    config = cfg

    if 'dse_cache_dir' not in config or 'dse_url' not in  config:
        raise ValueError("dse_cache_dir or dse_url are missing in cluster_config.json.")

    dse_binaries = "dse-{}-bin.tar.gz".format(config['revision'])

    # Create dse_cache_dir
    dse_cache = config['dse_cache_dir']
    if not os.path.exists(dse_cache):
        os.mkdir(dse_cache)

    if config["product"] == 'dse':
        download_binaries()

def download_binaries():
    "Parse config and download dse binaries (local)"

    # TODO since this is done locally on the cperf tool server, is there any possible concurrency
    # issue .. Or maybe we should simply keep a cache on each host? (Comment to remove)
    filename = os.path.join(dse_cache, dse_binaries)
    if os.path.exists(filename):
        print("Already in cache: {}".format(filename))
        return

    dse_url = config['dse_url']
    username = config['dse_username'] if 'dse_username' in config else None
    password = config['dse_password'] if 'dse_password' in config else None
    url = urljoin(dse_url, dse_binaries)
    request = download_file(url, filename, username, password)
    request.raise_for_status()

def get_dse_path():
    dirname = dse_binaries.replace('-bin.tar.gz', '')
    return os.path.join('fab', dirname)

def get_cassandra_path():
    return os.path.join(get_dse_path(), 'resources/cassandra/')

def get_bin_path():
    return os.path.join(get_dse_path(), 'bin')

def bootstrap(config):
    filename = os.path.join(dse_cache, dse_binaries)
    dest = os.path.join('fab', dse_binaries)

    # Upload the binaries
    fab.put(filename, dest)

    # Extract the binaries
    fab.run('tar -C fab -xf {}'.format(dest))

    return config['revision']

def start(config):
    dse_path = get_dse_path()
    dse_bin = os.path.join(dse_path, 'bin/dse')
    fab.puts("Starting DSE Cassandra..")
    cmd = 'JAVA_HOME={java_home} nohup {dse_bin} cassandra'.format(
        java_home=config['java_home'], dse_bin=dse_bin)
    fab.run(cmd)

def stop(clean):
    fab.run('jps | grep DseDaemon | cut -d" " -f1 | xargs kill -9', quiet=True)

def is_running():
    jps = fab.run('jps | grep DseDaemon"', quiet=True)
    return True if jps.return_code == 0 else False
