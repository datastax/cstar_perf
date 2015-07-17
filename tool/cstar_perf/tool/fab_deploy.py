"""Fabric file to setup a cluster so that fab_[cassandra|dse] can be used on it

Creates ~/.cstar_perf/cluster_config.json
Creates ~/fab directory on each node

This is currently only used by cstar_docker, but should remain useful
for other deployment types in the future.
"""

from fabric import api as fab
from fabric.tasks import execute as fab_execute
from StringIO import StringIO
import json
import re
import operator

fab.env.use_ssh_config = True
fab.env.connection_attempts = 10

def setup_hosts_file(hosts):
    """Setup /etc/hosts
    
    hosts is a dictionary of hostname -> ip address
    """
    with fab.settings(user='root'):
        for host, ip in hosts.items():
            fab.run("echo '{ip} {host}' >> /etc/hosts".format(ip=ip, host=host), quiet=True)
            
def setup_fab_dir(jdk_roots=['/usr/lib/jvm']):
    fab.run('rm -rf ~/fab && mkdir ~/fab')

    java_homes = []
    version_re = re.compile("^(openjdk|java) version \"(.*)\"$", re.MULTILINE)
    # Find available JDKs:
    for root in jdk_roots:
        homes = fab.run("find {root} -maxdepth 1 -mindepth 1 -type d".format(root=root), quiet=True).strip().split()
        for home in homes:
            # Find java binary and test that it runs:
            if fab.run('test -f {home}/bin/java && test -f {home}/bin/javac'.format(home=home), quiet=True).return_code == 0:
                version_out = fab.run("{home}/bin/java -version".format(home=home), quiet=True).replace("\r\n","\n")
                m = version_re.match(version_out)
                if m:
                    version = m.group(2)
                    java_homes.append((home, version))
    java_homes.sort(key=operator.itemgetter(1), reverse=True)
    assert len(java_homes) > 0, "Did not find any available JDKs"

    # Symlink JDKs in ~/fab/jvms
    fab.run("mkdir -p ~/fab/jvms")
    for home, version in java_homes:
        fab.run("ln -s {home} ~/fab/jvms/{version}".format(home=home, version=version))
    fab.run("ln -s {home} ~/fab/java".format(home=java_homes[0][0]))

    # Symlink ant
    assert fab.run("test -d /usr/share/ant", quiet=True).return_code == 0, "Did not find ant installed"
    fab.run("ln -s /usr/share/ant ~/fab/ant")
    
    # Checkout trunk cassandra-stress
    fab.run("mkdir -p ~/fab/stress")

    if fab.run("test -d ~/.docker_cassandra.git", quiet=True).return_code == 0:
        # Docker base image checks out cassandra for us, we can just copy that:
        fab.run("git -C ~/.docker_cassandra.git clean -fdx")
        fab.run("cp -a ~/.docker_cassandra.git/.git ~/fab/cassandra.git")
        fab.run("cp -a ~/.docker_cassandra.git ~/fab/stress/trunk")
    else:
        fab.run("git -C ~/fab/stress clone http://github.com/apache/cassandra.git trunk")
        fab.run("cp -a ~/fab/stress/trunk/.git ~/fab/cassandra.git")

    fab.run("git -C ~/fab/stress/trunk pull origin")
    fab.run("cd ~/fab/stress/trunk && JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8 JAVA_HOME=~/fab/java ~/fab/ant/bin/ant clean jar")
    fab.run("ln -s ~/fab/stress/trunk ~/fab/stress/default")
    
def copy_fab_dir(from_node):
    fab.run("rm -rf ~/fab ~/.m2")
    fab.run("rsync -a {from_node}:fab ~/".format(from_node=from_node))
    fab.run("rsync -a {from_node}:.m2 ~/".format(from_node=from_node))
    
def install_cstar_perf_tool(existing_checkout=False):
    if not existing_checkout:
        fab.run("mkdir -p ~/git")
        fab.run("git -C ~/git clone http://github.com/datastax/cstar_perf.git")
    with fab.settings(user='root'):
        fab.run("pip install -e /home/cstar/git/cstar_perf/tool")

    # Add all the git remotes and fetch:
    fab.run('fab -f ~/git/cstar_perf/tool/cstar_perf/tool/fab_cassandra.py -H {hosts} add_git_remotes'.format(
        hosts=",".join([fab.env.hosts] if isinstance(fab.env.hosts, basestring) else fab.env.hosts)))
    fab.run('git -C ~/fab/cassandra.git fetch --all')

def install_cstar_perf_frontend(existing_checkout=False):
    """Install the frontend

    This method assumes that Cassandra is already installed and running on the frontend node
    """
    if not existing_checkout:
        fab.run("mkdir -p ~/git")
        fab.run("git -C ~/git clone http://github.com/datastax/cstar_perf.git")
    with fab.settings(user='root'):
        fab.run("pip install -e /home/cstar/git/cstar_perf/frontend")

def copy_cluster_config(config):
    config = json.dumps(config, sort_keys=True, indent=4, separators=(',', ': '))
    config_file = StringIO(config)
    fab.run('mkdir -p ~/.cstar_perf')
    fab.put(config_file, '~/.cstar_perf/cluster_config.json')

