#!/usr/bin/env python2
import argparse
import subprocess
import shlex
import re
import os
import shutil
import sys
import json
from collections import defaultdict, OrderedDict
import tempfile
import time
from StringIO import StringIO
import paramiko
from distutils.version import LooseVersion

import logging
logging.basicConfig()
log = logging.getLogger('cstar_docker')
log.setLevel(logging.DEBUG)

from fabric import api as fab
from cstar_perf.tool import fab_deploy
from fabric.tasks import execute as fab_execute

CONTAINER_DEFAULT_MEMORY = '2G'

fab.env.user = 'cstar'

docker_image_name = 'datastax/cstar_docker'
# Dockerfile for cstar_perf, there are string format parameters in here:
#  ssh_pub_key - the text of the ssh public key
#  
dockerfile = """
FROM ubuntu:latest
MAINTAINER Ryan McGuire <ryan@datastax.com>

RUN \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get install -y \
      build-essential \
      software-properties-common \
      git \
      unzip \
      python \
      python-dev \
      python-pip \
      openssh-server \
      libssl-dev \
      ant \
      libjna-java \
      python-software-properties

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections && \
    add-apt-repository ppa:webupd8team/java && \
    apt-get update && \
    apt-get install -y \
      oracle-java8-installer \
      oracle-java7-installer \
      oracle-java8-set-default

# Download and compile cassandra, we don't use this verison, but what
# this does is provide a git cache and primes the ~/.m2 directory to
# speed things up:
RUN useradd -ms /bin/bash cstar
USER cstar
RUN git clone http://github.com/apache/cassandra.git ~/.docker_cassandra.git 
RUN cd ~/.docker_cassandra.git && \
    JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8 ant clean jar
USER root

#### Setup SSH
RUN mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    `### SSH login fix. Otherwise user is kicked off after login` && \
    sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

RUN mkdir -p /home/cstar/.ssh && \
  chmod 700 /home/cstar/.ssh && \
  echo '{ssh_pub_key}' > /home/cstar/.ssh/authorized_keys && \
  ssh-keygen -P '' -f /home/cstar/.ssh/id_rsa && \
  cat /home/cstar/.ssh/id_rsa.pub >> /home/cstar/.ssh/authorized_keys && \
  chmod 600 /home/cstar/.ssh/authorized_keys && \
  echo  'Host *' > /home/cstar/.ssh/config && \
  echo  '    StrictHostKeyChecking no' >> /home/cstar/.ssh/config &&\
  echo  '    UserKnownHostsFile=/dev/null' >> /home/cstar/.ssh/config && \
  chown -R cstar:cstar /home/cstar/.ssh

RUN mkdir -p /root/.ssh && \
  chmod 700 /root/.ssh && \
  cp /home/cstar/.ssh/authorized_keys /root/.ssh/authorized_keys && \
  cp /home/cstar/.ssh/id_rsa /root/.ssh/id_rsa && \
  cp /home/cstar/.ssh/config /root/.ssh/config

RUN mkdir -p /home/cstar/git/cstar_perf && \
    chown -R cstar:cstar /home/cstar/git && \
    mkdir -p /data/cstar_perf && \
    chown -R cstar:cstar /data
VOLUME ["/home/cstar/git/cstar_perf"]

### Expose SSH and Cassandra ports
EXPOSE 22 7000 7001 7199 9042 9160 61620 61621
CMD ["/usr/sbin/sshd", "-D"]
"""


def check_docker_version(expected_version='1.6.0'):
    version_cmd = shlex.split("docker --version")
    try:
        p = subprocess.Popen(version_cmd, stdout=subprocess.PIPE)
        version_string = p.communicate()[0]
    except OSError:
        raise AssertionError('Failed to run docker, it may not be installed?')
    m = re.match('Docker version ([^,]+), .*', version_string)
    if m:
        version = m.groups()[0]
        if LooseVersion(version) < expected_version:
            raise AssertionError(
                'Found docker version {}. This tool requires version {}+'.format(
                    version, expected_version))
        
def build_docker_image(tag=docker_image_name, force=False):
    ssh_pub_file=get_ssh_key_pair()[1]
    if force:
        rmi_cmd = shlex.split("docker rmi -f {} -".format(tag))
        log.info('Removing docker image...')
        p=subprocess.call(rmi_cmd)        

    build_cmd = shlex.split("docker build -t {} {} -".format(tag, '--no-cache' if force else ''))
    p=subprocess.Popen(build_cmd, stdin=subprocess.PIPE)
    with open(ssh_pub_file) as f:
        p.communicate(dockerfile.format(ssh_pub_key=f.read().strip()))
    
def get_container_data(container):
    inspect_cmd = shlex.split("docker inspect {}".format(container))
    p = subprocess.Popen(inspect_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        return json.loads(p.communicate()[0])[0]
    except IndexError:
        raise AssertionError('No docker container or image with id: {}'.format(container))

def get_ssh_key_pair():
    """Create a cstar_docker specific SSH key, or return the previously generated one"""
    key_path = os.path.join(os.path.expanduser("~"), ".cstar_perf","cstar_docker_key")
    pub_key_path = key_path + '.pub'
    if not (os.path.exists(key_path) and os.path.exists(pub_key_path)):
        key = paramiko.rsakey.RSAKey.generate(2048)
        with open(key_path, 'w') as f:
            key.write_private_key(f)
        with open(pub_key_path, 'w') as f:
            f.write("ssh-rsa ")
            f.write(key.get_base64())
            f.write(" cstar_docker generated {}".format(time.ctime()))
            f.write("\n")
        os.chmod(key_path, 0600)
        os.chmod(pub_key_path, 0600)
    return (key_path, pub_key_path)

def get_clusters(cluster_regex='all', all_metadata=False):
    """Get all clusters matching the cluster name regex.

    Returns a list of names, unless all_metadata=True, then a map of
    all container inspection data is returned.
    """
    clusters = defaultdict(list) # {cluster_name : [first_node_metadata, 2nd...], ...}
    cluster_nodes = defaultdict(list)
    p = subprocess.Popen(shlex.split("docker ps -aq"), stdout=subprocess.PIPE)
    containers = p.communicate()[0].strip()
    class NoContainersException(Exception):
        pass
    try:
        if containers == '':
            raise NoContainersException
        containers = containers.split('\n')
        for container in containers:
            data = get_container_data(container)
            try:
                labels = data['Config']['Labels']
                if labels['cstar_node'] == 'true':
                    container_name = data['Name'] = data['Name'].lstrip('/')
                    node_num = labels['node'] = int(labels['node'])
                    if cluster_regex.lower() == 'all' or re.match(cluster_regex, container_name):
                        clusters[labels['cluster_name']].append(data)
                        cluster_nodes[labels['cluster_name']].append(container_name)
            except KeyError:
                pass
    except NoContainersException:
        pass   

    # Sort cluster lists by node number:
    for cluster_name, cluster_data  in clusters.items():
        cluster_data.sort(key=lambda x:x['Config']['Labels']['node'])
        # spot check for inconsistencies:
        cluster_types = set([x['Config']['Labels']['cluster_type'] for x in cluster_data])
        assert len(cluster_types) == 1, "{} has more than one cluster_type: {}".format(cluster_name, cluster_types)
    for cluster_name, nodes in cluster_nodes.items():
        nodes.sort()

        
    if all_metadata:
        return clusters
    else:
        return cluster_nodes


def launch(num_nodes, cluster_name='cnode', destroy_existing=False,
           install_tool=True, frontend=False, mount_host_src=False, verbose=False,
           client_double_duty=False):
    """Launch cluster nodes, return metadata (ip addresses etc) for the nodes"""

    assert num_nodes > 0, "Cannot start a cluster with {} nodes".format(num_nodes)
    if frontend:
        assert num_nodes == 1 and client_double_duty, "Can only start a frontend with a single node"
        
    cluster_type = 'frontend' if frontend else 'cluster'
        
    try:
        get_container_data(docker_image_name)
    except AssertionError:
        print("The docker image {} was not found, build the docker image first "
              "with: 'cstar_docker build'".format(docker_image_name))
        exit(1)
    existing_nodes = get_clusters(cluster_name)
    if len(existing_nodes):
        if destroy_existing:
            destroy(cluster_name)
        else:
            log.error('Cannot launch cluster \'{}\' as it already exists.'.format(cluster_name))
            log.error('You must destroy the existing cluster, or use --destroy-existing '
                      'in your launch command')
            exit(1)

    first_cassandra_node = 1
    if client_double_duty:
        first_cassandra_node = 0
        log.info('Launching a {} node cluster...'.format(num_nodes))
    else:        
        # We need one more node than requested to run the client
        num_nodes += 1
        log.info('Launching a {} node cluster with a separate client node ...'.format(num_nodes))
    node_data = OrderedDict()
    for i in range(num_nodes):
        node_name = "%s_%02d" % (cluster_name,i)
        ssh_path = os.path.split(get_ssh_key_pair()[0])[0]
        run_cmd = ('docker run --label cstar_node=true --label '
            'cluster_name={cluster_name} --label cluster_type={cluster_type} --label node={node_num} '
            '-d -m {CONTAINER_DEFAULT_MEMORY} --name={node_name} -h {node_name}'.format(
                cluster_name=cluster_name, node_num=i, node_name=node_name, cluster_type=cluster_type,
                CONTAINER_DEFAULT_MEMORY=CONTAINER_DEFAULT_MEMORY, ssh_path=ssh_path))
        if mount_host_src:
            # Try to find the user's git clone of cstar_perf:
            candidates = [
                # Get the directory relative to this file - only works
                # if user installed in-place (pip install -e)
                os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, os.pardir)),
                # In the current directory:
                os.getcwd()
            ]
            for d in candidates:
                if os.path.exists(os.path.join(d, '.git')) and \
                   os.path.exists(os.path.join(d, 'tool')) and \
                   os.path.exists(os.path.join(d, 'frontend')):
                    cstar_dir = d
                    break
            else:
                log.error("Could not mount your git checkout of cstar_perf because none could be found. Try installing cstar_perf in developer mode: 'pip install -e ./tool' or try running cstar_docker from the same directory as your checkout")
                exit(1)
            run_cmd = run_cmd + " -v {cstar_dir}:/home/cstar/git/cstar_perf".format(cstar_dir=cstar_dir)
        run_cmd = run_cmd + ' ' + docker_image_name
        log.debug(run_cmd)
        p=subprocess.Popen(shlex.split(run_cmd),
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        container_id = p.communicate()[0].strip()
        node_data[node_name] = get_container_data(container_id)
    hosts = OrderedDict()
    for name, data in node_data.items():
        hosts[name] = data['NetworkSettings']['IPAddress']

    # Write /etc/hosts
    with fab.settings(hosts=[n for n in hosts.values()]):
        fab_execute(fab_deploy.setup_hosts_file, hosts)

    if frontend:
        log.info("Installing cstar_perf.frontend ... ")
        try:
            __install_cstar_perf_frontend(cluster_name, hosts, mount_host_src=mount_host_src)
        except:
            destroy(cluster_name)
            raise
    elif install_tool:
        log.info("Installing cstar_perf.tool ... ")
        try:
            __install_cstar_perf_tool(cluster_name, hosts, mount_host_src=mount_host_src,
                                      first_cassandra_node=first_cassandra_node)
        except:
            destroy(cluster_name)
            raise
            
    if verbose:
        print("Started {} nodes:".format(num_nodes))
        print("")
        info(cluster_name)
    return node_data

def __install_cstar_perf_frontend(cluster_name, hosts, mount_host_src=False):
    assert len(hosts) == 1, "Cannot install frontend onto more than one node"
    host, ip = hosts.popitem()
    with fab.settings(hosts=ip):
        # Setup cstar_perf.tool, not normally needed on the frontend, but we'll use it to
        # easily bootstrap the frontend's C* backend:
        fab_execute(fab_deploy.setup_fab_dir)
        __install_cstar_perf_tool(cluster_name, hosts, mount_host_src=mount_host_src, first_cassandra_node=0)        

        # Install the frontend as well as Cassandra to hold the frontend DB
        fab_execute(fab_deploy.install_cstar_perf_frontend, existing_checkout=mount_host_src, bootstrap_cassandra=True)
    
def __install_cstar_perf_tool(cluster_name, hosts, mount_host_src=False, first_cassandra_node=None):
    first_node = hosts.values()[0]
    other_nodes = hosts.values()[1:]

    if first_cassandra_node is None:
        # If a first cluster node was not explicitly set, assume we
        # mean the second node of the cluster, unless it's a single
        # node cluster, then it's node 0.
        if len(hosts) > 1:
            first_cassandra_node = 1
        else:
            first_cassandra_node = 0
            
    # Create the cluster config file
    cluster_config = {
        "block_devices": [],
        "blockdev_readahead": None,
        "hosts": {
            host : {
                "hostname": host,
                "internal_ip": ip,
                "external_ip": ip,
                "seed": True,
                "datacenter": 'dc1'
            } for host, ip in hosts.items()[first_cassandra_node:]
        },
        "name": cluster_name,
        "stress_node": first_node,
        "user":"cstar",
        "data_file_directories": ['/data/cstar_perf/data'],
        "commitlog_directory": '/data/cstar_perf/commitlog',
        "saved_caches_directory": '/data/cstar_perf/saved_caches'
    }
    
    with fab.settings(hosts=first_node):
        fab_execute(fab_deploy.copy_cluster_config, cluster_config)

    # Setup ~/fab directory (java, ant, stress, etc) on the first node
    with fab.settings(hosts=first_node):
        fab_execute(fab_deploy.setup_fab_dir)
        # Install cstar_perf
        fab_execute(fab_deploy.install_cstar_perf_tool, existing_checkout=mount_host_src)
    # rsync ~/fab to the other nodes:
    if len(other_nodes) > 0:
        with fab.settings(hosts=other_nodes):
            fab_execute(fab_deploy.copy_fab_dir, first_node)

def info(cluster_name):
    clusters = get_clusters(cluster_name, all_metadata=True)
    containers = clusters[cluster_name]
    node_names = [n['Name'] for n in containers]
    if len(containers) == 0:
        print("No cluster named {} found".format(cluster_name))
    else:
        print("Cluster: {}, {} nodes".format(cluster_name, len(node_names)))
        for n, node_name in enumerate(node_names):
            data = containers[n]
            if data['State']['Running']:
                print("    {} : {}".format(node_name, data['NetworkSettings']['IPAddress']))
            else:
                print("    {} : offline".format(node_name))
                
def destroy(cluster_regex):
    """Destroy clusters"""
    clusters = get_clusters(cluster_regex)
    for cluster, containers in clusters.items():
        if len(containers) > 0:
            log.info('Destroying {} containers...'.format(cluster_regex))
        for container in containers:
            destroy_cmd = shlex.split("docker rm -f {}".format(container))
            subprocess.call(destroy_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def __update_node_ip_addresses(cluster_name):
    """Update node ip addresses

    This is necessary because docker assigns new IP addresses each time a container is restarted
    """
    # Retrieve the current ~/.cstar_perf/cluster_config.json on node 00:
    clusters = get_clusters(cluster_name, all_metadata=True)
    cluster = clusters[cluster_name]
    current_ips = dict([(c['Name'], c['NetworkSettings']['IPAddress']) for c in cluster])
    node0 = cluster[0]['Name']
    with fab.settings(hosts=current_ips[node0]):
        def get_cluster_config():
            cfg = StringIO()
            fab.get("~/.cstar_perf/cluster_config.json", cfg)
            cfg.seek(0)
            return json.load(cfg)
        cluster_config = fab_execute(get_cluster_config).values()[0]

    # Update cluster_config with the current node IP addresses:
    for host, cfg in cluster_config['hosts'].items():
        cluster_config['hosts'][host]['internal_ip'] = cluster_config['hosts'][host]['external_ip'] = current_ips[host]

    cluster_config['stress_node'] = current_ips[node0]

    # Replace the config file onto node 0:
    with fab.settings(hosts=cluster[0]['NetworkSettings']['IPAddress']):
        def put_cluster_config():
            cfg = StringIO()
            json.dump(cluster_config, cfg, indent=2)
            fab.put(cfg, "~/.cstar_perf/cluster_config.json")
        fab_execute(put_cluster_config)
        
def start(cluster_name):
    """start clusters"""
    clusters = get_clusters(cluster_name)
    cluster = clusters[cluster_name]
    for container in cluster:
        start_cmd = shlex.split("docker start {}".format(container))
        subprocess.call(start_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    __update_node_ip_addresses(cluster_name)

def stop(cluster_name):
    """stop clusters"""
    clusters = get_clusters(cluster_name)
    cluster = clusters[cluster_name]
    for container in cluster:
        stop_cmd = shlex.split("docker stop {}".format(container))
        subprocess.call(stop_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
def list_clusters():
    """List clusters"""
    clusters = get_clusters('all', all_metadata=True)
    for cluster, containers in clusters.items():
        print("{name}, {num_nodes} node{plural} ({cluster_type})".format(
            name=cluster, num_nodes=len(containers),
            cluster_type=containers[0]['Config']['Labels']['cluster_type'],
            plural="s" if len(containers) > 1 else ""))

def ssh(cluster_name, node, user='cstar', ssh_key_path=os.path.join(os.path.expanduser("~"),'.ssh','id_rsa')):
    clusters = get_clusters(cluster_name, all_metadata=True)
    containers = clusters[cluster_name]
    node_names = [c['Name'] for c in containers]
    if len(containers) == 0:
        print("No cluster named {} found".format(cluster_name))
    elif containers[node]['State']['Running'] is False:
        log.error("Node is not running. Try starting the cluster: cstar_docker start {}".format(cluster_name))
    else:
        command = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=%s -o User=%s -i %s %s' \
                  % (os.devnull,
                     user,
                     get_ssh_key_pair()[0],
                     containers[node]['NetworkSettings']['IPAddress'])
        proc = subprocess.Popen(command, shell=True)
        proc.wait()
    
        
def execute_cmd(cmd, args):
    if cmd == 'launch':
        launch(args.num_nodes, cluster_name=args.name,
               destroy_existing=args.destroy_existing,
               install_tool=not args.no_install,
               mount_host_src=args.mount, verbose=True,
               client_double_duty=args.client_double_duty)
    elif cmd == 'frontend':
        launch(1, cluster_name=args.name,
               destroy_existing=args.destroy_existing,
               frontend=True, client_double_duty=True,
               mount_host_src=args.mount, verbose=True)
    elif cmd == 'associate':
        raise NotImplementedError('todo')
    elif cmd == 'start':
        start(cluster_name=args.name)
    elif cmd == 'stop':
        stop(cluster_name=args.name)
    elif cmd == 'destroy':
        destroy(args.cluster_regex)
    elif cmd == 'list':
        list_clusters()
    elif cmd == 'info':
        info(args.cluster_name)
    elif cmd == 'ssh':
        ssh(args.cluster_name, args.node, user=args.user)
    elif cmd == 'build':
        build_docker_image(force=args.force)
    else:
        raise AssertionError('Unknown command: {cmd}'.format(cmd=cmd))

    
def main():
    parser = argparse.ArgumentParser(description='cstar_docker.py - '
                                     'Interact with cstar_perf docker clusters',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser_subparsers = parser.add_subparsers(dest='command')

    launch = parser_subparsers.add_parser('launch', description="Launch a cluster with the given name and number of nodes")
    launch.add_argument('name', help='The name of the cluster')
    launch.add_argument('num_nodes', type=int, help='The number of Cassandra nodes to launch')
    launch.add_argument('-c', '--client-double-duty', action="store_true", help='Use node 00 as another Cassandra node, in addition to running the client')
    launch.add_argument(
        '--no-install', help='Don\'t install cstar_perf.tool', action='store_true')
    launch.add_argument(
        '-m', '--mount', help='Mount the host system\'s cstar_perf checkout rather than install from github', action='store_true')
    launch.add_argument(
        '--destroy-existing', help='Destroy any existing cluster with the same name before launching', action="store_true")
    
    frontend = parser_subparsers.add_parser('frontend', description="Launch a single node frontend instance")
    frontend.add_argument('name', help='The name of the frontend node')
    frontend.add_argument(
        '-m', '--mount', help='Mount the host system\'s cstar_perf checkout rather than install from github', action='store_true')
    frontend.add_argument(
        '--destroy-existing', help='Destroy any existing cluster with the same name before launching', action="store_true")

    associate = parser_subparsers.add_parser('associate', description="Hook up one or more clusters to a frontend")
    associate.add_argument('frontend', help='The name of the frontend node')
    associate.add_argument('clusters', help='The names of the clusters to hook up to the frontend', nargs='+')    
    
    destroy = parser_subparsers.add_parser('destroy', description='Destroy clusters - specify a regex of cluster names to destroy, or specify \'all\' to destroy all clusters created')
    destroy.add_argument('cluster_regex', help='The regex of the names of clusters to destroy')

    list_clusters = parser_subparsers.add_parser('list', description='List clusters')

    info = parser_subparsers.add_parser('info', description='Print cluster information')
    info.add_argument('cluster_name', help='The name of the cluster')

    ssh = parser_subparsers.add_parser('ssh', description='SSH to cluster node')
    ssh.add_argument('cluster_name', help='The name of the cluster')
    ssh.add_argument('node', help='The node number', type=int)
    ssh.add_argument('-u', '--user', help='User to login as (default: cstar)', default='cstar')

    build = parser_subparsers.add_parser('build', description='Build the Docker image')
    build.add_argument(
        '-f', '--force', help='Force building the image by removing any existing image first', action='store_true')

    start = parser_subparsers.add_parser('start', description='Start an existing cluster')
    start.add_argument('name', help='The name of the cluster to start')

    stop = parser_subparsers.add_parser('stop', description='Stop an existing cluster')
    stop.add_argument('name', help='The name of the cluster to stop')
    
    try:
        args = parser.parse_args()
    finally:
        # Print verbose help if they didn't give any command:
        if len(sys.argv) == 1:
            parser.print_help()

    check_docker_version()
    execute_cmd(args.command, args)

fab.env.key_filename = get_ssh_key_pair()[0]
    
if __name__ == "__main__":
    main()
