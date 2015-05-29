#!/usr/bin/env python2
import argparse
import subprocess
import shlex
import re
import os
import shutil
import sys
import json
from collections import defaultdict
import tempfile
from distutils.version import LooseVersion

import logging
logging.basicConfig()
log = logging.getLogger('cstar_docker')
log.setLevel(logging.INFO)

docker_image_name = 'datastax/cstar_docker'
# Dockerfile for cstar_perf, there are string format parameters in here:
#  ssh_pub_key - the text of the ssh public key
#  
dockerfile = """
FROM ubuntu:latest
MAINTAINER Ryan McGuire <ryan@datastax.com>

RUN \
  apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0x219BD9C9 && \
  echo "deb http://repos.azulsystems.com/ubuntu `lsb_release -cs` main" >> /etc/apt/sources.list.d/zulu.list && \
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
      zulu-8 \
      zulu-7 \
      libjna-java

#### Setup SSH
RUN mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    `### SSH login fix. Otherwise user is kicked off after login` && \
    sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

RUN useradd -ms /bin/bash cstar && \
  mkdir -p /home/cstar/.ssh && \
  chmod 700 /home/cstar/.ssh && \
  echo '{ssh_pub_key}' > /home/cstar/.ssh/authorized_keys && \
  chmod 600 /home/cstar/.ssh/authorized_keys && \
  chown -R cstar:cstar /home/cstar/.ssh

RUN mkdir -p /root/.ssh && \
  chmod 700 /root/.ssh && \
  echo '{ssh_pub_key}' > /root/.ssh/authorized_keys && \
  chmod 600 /home/cstar/.ssh/authorized_keys

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
    """Find user's ssh key"""
    candidates = [os.path.join(os.path.expanduser('~'),'.ssh',x) for x in
                      ('id_rsa.pub','id_dsa.pub','id_ecdsa.pub')]
    for candidate in candidates:
        if os.path.exists(candidate):
            ssh_pub_file = candidate
            break
    else:
        raise AssertionError('Could not find your SSH key, tried : {}'.format(candidates))
    return (ssh_pub_file.replace('.pub',''), ssh_pub_file)

def get_containers(cluster_regex='all', all_metadata=False):
    """Get all containers matching the cluster name regex.

    Returns a list of names, unless all_metadata=True, then a map of
    all container inspection data is returned.
    """
    container_names = []
    container_data = {} # node_name => container metadata
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
                if data['Config']['Labels']['cstar_node'] == 'true':
                    container_name = data['Name'].lstrip('/')
                    if cluster_regex.lower() == 'all' or re.match(cluster_regex, container_name):
                        container_names.append(container_name)
                        container_data[container_name] = data
            except KeyError:
                pass
    except NoContainersException:
        pass   
    
    if all_metadata:
        return container_data
    else:
        return container_names

def launch(num_nodes, cluster_name='cnode', destroy_existing=False, install_frontend=False, install_tool=True, verbose=False):
    """Launch cluster nodes, return metadata (ip addresses etc) for the nodes"""

    if install_frontend and not install_tool:
        log.error("Cannot install cstar_perf.frontend without also installing cstar_perf.tool")
        exit(1)
    
    try:
        get_container_data(docker_image_name)
    except AssertionError:
        print("The docker image {} was not found, build the docker image first "
              "with: 'cstar_docker build'".format(docker_image_name))
        exit(1)
    existing_nodes = get_containers(cluster_name)
    if len(existing_nodes):
        if destroy_existing:
            destroy(cluster_name)
        else:
            log.error('Cannot launch cluster \'{}\' as it already exists.'.format(cluster_name))
            log.error('You must destroy the existing cluster, or use --destroy-existing '
                      'in your launch command')
            exit(1)

    log.info('Launching a {} node cstar_perf cluster...'.format(num_nodes))
    node_data = {}
    for i in range(num_nodes):
        node_name = "%s_%02d" % (cluster_name,i)
        ssh_path = os.path.split(get_ssh_key_pair()[0])[0]
        p=subprocess.Popen(shlex.split(
            'docker run --label cstar_node=true --label '
            'cluster_name={cluster_name} --label node={node_num} -d -m 256M --name={node_name} '
            '-h {node_name} {docker_image_name}'.format(
                cluster_name=cluster_name, node_num=i, node_name=node_name,
                ssh_path=ssh_path, docker_image_name=docker_image_name)),
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        container_id = p.communicate()[0].strip()
        node_data[node_name] = get_container_data(container_id)

    if install_tool:
        log.info("Installing cstar_perf.tool ... ")
    if install_frontend:
        log.info("Installing cstar_perf.frontend ... ")
            
    if verbose:
        print("Started {} nodes:".format(num_nodes))
        print("")
        info(cluster_name)
    return node_data

def info(cluster_name):
    containers = get_containers(cluster_name, all_metadata=True)
    node_names = sorted((n for n in containers))
    if len(containers) == 0:
        print("No cluster named {} found".format(cluster_name))
    else:
        print("Cluster: {}, {} nodes".format(cluster_name, len(node_names)))
        for node in node_names:
            data = containers[node]
            print("    {} : {}".format(node, data['NetworkSettings']['IPAddress']))


def destroy(cluster_regex):
    """Destroy clusters"""
    containers = get_containers(cluster_regex)
    if len(containers) > 0:
        log.info('Destroying {} containers...'.format(cluster_regex))
    for container in containers:
        destroy_cmd = shlex.split("docker rm -f {}".format(container))
        subprocess.call(destroy_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def list_clusters():
    """List clusters"""
    containers = get_containers('all', all_metadata=True)
    cluster_containers = defaultdict(list)
    for container in containers.values():
        cluster_name = container['Config']['Labels']['cluster_name']
        cluster_containers[cluster_name].append(container['Id'])
    clusters = sorted([c for c in cluster_containers.keys()])
    for cluster in clusters:
        print("{}, {} instances".format(cluster, len(cluster_containers[cluster])))

def ssh(cluster_name, node, user='cstar', ssh_key_path=os.path.join(os.path.expanduser("~"),'.ssh','id_rsa')):
    containers = get_containers(cluster_name, all_metadata=True)
    node_names = sorted((n for n in containers))
    if len(containers) == 0:
        print("No cluster named {} found".format(cluster_name))
    else:
        command = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=%s -o User=%s -i %s %s' \
                  % (os.devnull,
                     user,
                     get_ssh_key_pair()[0],
                     containers[node_names[node]]['NetworkSettings']['IPAddress'])
        proc = subprocess.Popen(command, shell=True)
        proc.wait()
    
        
def execute_cmd(cmd, args):
    if cmd == 'launch':
        launch(args.num_nodes, cluster_name=args.name,
               destroy_existing=args.destroy_existing,
               install_frontend=args.frontend, install_tool=not args.no_install, verbose=True)
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
    launch.add_argument('num_nodes', type=int, help='The number of nodes')
    launch.add_argument(
        '--no-install', help='Don\'t install cstar_perf.tool', action='store_true')
    launch.add_argument(
        '-f', '--frontend', help='Install cstar_perf.frontend, the web frontend (tool only is installed by default)', action='store_true')
    launch.add_argument(
        '--destroy-existing', help='Destroy any existing cluster with the same name before launching', action="store_true")

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
    
    try:
        args = parser.parse_args()
    finally:
        # Print verbose help if they didn't give any command:
        if len(sys.argv) == 1:
            parser.print_help()

    check_docker_version()
    execute_cmd(args.command, args)
    
if __name__ == "__main__":
    main()
