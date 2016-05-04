#!/usr/bin/env python2
import argparse
import subprocess
import shlex
import re
import os
import sys
import json
import hashlib
from collections import defaultdict, OrderedDict
import time
from StringIO import StringIO
import webbrowser
from distutils.version import LooseVersion
import logging

import paramiko

logging.basicConfig()
log = logging.getLogger('cstar_docker')
log.setLevel(logging.DEBUG)

from fabric import api as fab
from fabric.contrib.files import append as fab_append
from cstar_perf.tool import fab_deploy
from fabric.tasks import execute as fab_execute

import tasks

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
      sudo \
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
      psmisc \
      python-software-properties \
      libjpeg-dev \
      lxc

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    add-apt-repository ppa:webupd8team/java && \
    apt-get update && \
    apt-get install -y \
      oracle-java8-installer \
      oracle-java7-installer \
      oracle-java8-set-default

# Download and compile cassandra, we don't use this verison, but what
# this does is provide a git cache and primes the ~/.m2 directory to
# speed things up:
RUN groupadd -g 999 docker
RUN useradd -ms /bin/bash -G docker cstar
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

RUN echo "%wheel        ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    groupadd wheel && \
    gpasswd -a cstar wheel

### Expose SSH and Cassandra ports
EXPOSE 22 7000 7001 7199 9042 9160 61620 61621

RUN pip install supervisor
RUN echo "[unix_http_server]" > /supervisord.conf && \
    echo "file=/tmp/supervisor.sock"                                                        >> /supervisord.conf && \
    echo ""                                                                                 >> /supervisord.conf && \
    echo "[supervisord]"                                                                    >> /supervisord.conf && \
    echo "logfile=/tmp/supervisord.log "                                                    >> /supervisord.conf && \
    echo "logfile_maxbytes=50MB        "                                                    >> /supervisord.conf && \
    echo "logfile_backups=10           "                                                    >> /supervisord.conf && \
    echo "loglevel=info                "                                                    >> /supervisord.conf && \
    echo "pidfile=/tmp/supervisord.pid "                                                    >> /supervisord.conf && \
    echo "nodaemon=false               "                                                    >> /supervisord.conf && \
    echo "minfds=1024                  "                                                    >> /supervisord.conf && \
    echo "minprocs=200                 "                                                    >> /supervisord.conf && \
    echo ""                                                                                 >> /supervisord.conf && \
    echo "[rpcinterface:supervisor]"                                                        >> /supervisord.conf && \
    echo "supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface" >> /supervisord.conf && \
    echo ""                                                                                 >> /supervisord.conf && \
    echo ""                                                                                 >> /supervisord.conf && \
    echo "[supervisorctl]"                                                                  >> /supervisord.conf && \
    echo "serverurl=unix:///tmp/supervisor.sock "                                           >> /supervisord.conf && \
    echo ""                                                                                 >> /supervisord.conf && \
    echo "[program:sshd]"                                                                   >> /supervisord.conf && \
    echo "command=/usr/sbin/sshd -D"                                                        >> /supervisord.conf && \
    echo "user=root"                                                                        >> /supervisord.conf && \
    echo "autostart=true"                                                                   >> /supervisord.conf && \
    echo "autorestart=true"                                                                 >> /supervisord.conf && \
    echo "redirect_stderr=true"                                                             >> /supervisord.conf

### install the C* driver without any extensions to speed up installation time
RUN CASS_DRIVER_NO_EXTENSIONS=1 pip install cassandra-driver

CMD ["supervisord", "-n", "-c", "/supervisord.conf"]
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

def get_dockerfile():
    ssh_pub_file=get_ssh_key_pair()[1]
    with open(ssh_pub_file) as f:
        return dockerfile.format(ssh_pub_key=f.read().strip())
        
def build_docker_image(tag=docker_image_name, force=False):
    if force:
        rmi_cmd = shlex.split("docker rmi -f {} -".format(tag))
        log.info('Removing docker image...')
        p=subprocess.call(rmi_cmd)        

    build_cmd = shlex.split("docker build -t {} {} -".format(tag, '--no-cache' if force else ''))
    p=subprocess.Popen(build_cmd, stdin=subprocess.PIPE)
    p.communicate(get_dockerfile())
    if p.returncode == 0:
        # Save the hash of the dockerfile so we can know if we need to
        # rebuild the image:
        dockerfile_hash = os.path.join(os.path.expanduser("~"), ".cstar_perf","cstar_docker_image_hash")
        docker_image_hash = hashlib.sha256(get_dockerfile()).hexdigest()
        with open(dockerfile_hash, 'w') as f:
            f.write(docker_image_hash)

def check_if_build_necessary(exit_if_not_ready=True):
    """Checks the previous hash of the dockerfile against the latest
    version to determine if a rebuild is nescessary"""
    current_dockerfile_hash = hashlib.sha256(get_dockerfile()).hexdigest()
    try:
        with open(os.path.join(os.path.expanduser("~"), ".cstar_perf","cstar_docker_image_hash")) as f:
            previous_dockerfile_hash = f.read().strip()
        needs_rebuild = not current_dockerfile_hash == previous_dockerfile_hash
    except IOError:
        needs_rebuild = True
    if needs_rebuild and exit_if_not_ready:
        print("The Dockerfile has changed since you last built the image. You must rebuild your image:")
        print ("   cstar_docker build")
        exit(1)
        
def get_container_data(container):
    inspect_cmd = shlex.split("docker inspect {}".format(container))
    p = subprocess.Popen(inspect_cmd, stdout=subprocess.PIPE)
    try:
        return json.loads(p.communicate()[0])[0]
    except IndexError:
        raise AssertionError('No docker container or image with id: {}'.format(container))

def get_ssh_key_pair():
    """Create a cstar_docker specific SSH key, or return the previously generated one"""
    key_path = os.path.join(os.path.expanduser("~"), ".cstar_perf","cstar_docker_key")
    pub_key_path = key_path + '.pub'
    if not (os.path.exists(key_path) and os.path.exists(pub_key_path)):
        try:
            os.mkdir(os.path.join(os.path.expanduser("~"), ".cstar_perf"))
        except IOError:
            pass
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
    cluster_regex = cluster_regex + ("" if cluster_regex.endswith("$") else "$")
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
                if labels and labels['cstar_node'] == 'true':
                    container_name = data['Name'] = data['Name'].lstrip('/')
                    node_num = labels['node'] = int(labels['node'])
                    if cluster_regex.lower() == 'all$' or re.match(cluster_regex, labels['cluster_name']):
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

def get_ips(cluster_name):
    clusters = get_clusters(cluster_name, all_metadata=True)
    cluster = clusters[cluster_name]
    return tuple((c['Name'], c['NetworkSettings']['IPAddress']) for c in cluster)

def check_cluster_exists(cluster_regex):
    existing_nodes = get_clusters(cluster_regex)
    return bool(len(existing_nodes))

def launch(num_nodes, cluster_name='cnode', destroy_existing=False,
           install_tool=True, frontend=False, mount_host_src=False, verbose=False,
           client_double_duty=False):
    """Launch cluster nodes, return metadata (ip addresses etc) for the nodes"""
    if '_' in cluster_name:
        raise ValueError('Please use a cluster name without underscores. The cluster name is also used for the hostname and newer docker versions do not support underscores in the hostname!')

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
    check_if_build_necessary()

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
        # newer docker versions don't support underscores in the hostname
        node_name = "%s-%02d" % (cluster_name, i)
        ssh_path = os.path.split(get_ssh_key_pair()[0])[0]
        run_cmd = ('docker run --ulimit memlock=100000000:100000000 --privileged --label cstar_node=true --label '
            'cluster_name={cluster_name} --label cluster_type={cluster_type} --label node={node_num} '
            ' -v /var/run/docker.sock:/var/run/docker.sock -v /usr/bin/docker:/bin/docker '
            '-d -m {CONTAINER_DEFAULT_MEMORY} --name={node_name} {port_settings} -h {node_name}'.format(
                cluster_name=cluster_name, node_num=i, node_name=node_name, cluster_type=cluster_type,
                CONTAINER_DEFAULT_MEMORY=CONTAINER_DEFAULT_MEMORY, ssh_path=ssh_path,
                port_settings="-p 127.0.0.1:8000:8000" if frontend else ""))
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
                           stdout=subprocess.PIPE)
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
        __install_cstar_perf_frontend(cluster_name, hosts, mount_host_src=mount_host_src)
    elif install_tool:
        log.info("Installing cstar_perf.tool ... ")
        __install_cstar_perf_tool(cluster_name, hosts, mount_host_src=mount_host_src,
                                      first_cassandra_node=first_cassandra_node)            
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
        __install_cstar_perf_tool(cluster_name, {host:ip}, mount_host_src=mount_host_src, first_cassandra_node=0)        
        # Setup C* and add it to the supervisor to start on boot:
        def setup_cassandra():
            __update_node_ip_addresses(cluster_name, static_ips={host:'127.0.0.1'})
            fab.run("cstar_perf_bootstrap -v cassandra-2.2.6")
        with fab.settings(hosts=ip):
            fab_execute(setup_cassandra)
        def setup_boot_items():
            boot_items = "\n".join([
                '',
                '[program:cassandra]',
                'command=/home/cstar/fab/cassandra/bin/cassandra -f',
                'priority=1',
                'user=cstar',
                'autostart=true',
                'autorestart=false',
                'redirect_stderr=true',
                '',
                '[program:cstar_perf_notifications]',
                'command=cstar_perf_notifications -F',
                'priority=1',
                'user=cstar',
                'autostart=true',
                'autorestart=true',
                'startretries=30',
                'redirect_stderr=true',
                '',
                '[program:cstar_perf_server]',
                'command=cstar_perf_server',
                'priority=2',
                'user=cstar',
                'environment=HOME=/home/cstar',
                'autostart=true',
                'startretries=30',
                'autorestart=true',
                'redirect_stderr=true',
                ''
            ])
            fab_append("/supervisord.conf", boot_items)
        with fab.settings(hosts=ip, user="root"):
            fab_execute(setup_boot_items)

        # Install the frontend as well as Cassandra to hold the frontend DB
        fab_execute(fab_deploy.install_cstar_perf_frontend)

        # Generate and save the credentials
        with fab.settings(hosts=ip):
            fab_execute(fab_deploy.generate_frontend_credentials)

        # Restart the container so all the auto boot stuff is applied:
        subprocess.call(shlex.split("docker restart {}".format(host)))

        # Post Restart setup
        frontend_name, frontend_ip = get_ips(cluster_name)[0]
        with fab.settings(hosts=frontend_ip):
            fab_execute(fab_deploy.create_default_frontend_users)

        log.info("cstar_perf service started, opening in your browser: http://localhost:8000")
        webbrowser.open("http://localhost:8000")
        log.info("Log in with email: admin@example.com and password: admin")
        log.info("You will need to use the 'cstar_docker associate' command to link up a cluster")
        
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
        "saved_caches_directory": '/data/cstar_perf/saved_caches',
        'cdc_directory': '/data/cstar_perf/cdc',
        'cdc_overflow_directory': '/data/cstar_perf/cdc_overflow',
        "docker": True
    }
    
    with fab.settings(hosts=first_node):
        fab_execute(fab_deploy.copy_cluster_config, cluster_config)

    # Setup ~/fab directory (java, ant, stress, etc) on the first node
    with fab.settings(hosts=first_node):
        fab_execute(fab_deploy.setup_fab_dir)
        # Install cstar_perf
        fab_execute(fab_deploy.install_cstar_perf_tool)
        # Install cstar_perf.frontend
        fab_execute(fab_deploy.install_cstar_perf_frontend)
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
            subprocess.call(destroy_cmd, stdout=subprocess.PIPE)

def associate(frontend_name, cluster_names, with_dse=False):

    try:
        frontend = get_clusters(frontend_name, all_metadata=True)[frontend_name][0]
    except IndexError:
        raise ValueError("No frontend cluster named {} found".format(frontend_name))

    clusters = []
    for c in cluster_names:
        try:
            cluster = get_clusters(c, all_metadata=True)[c][0]
        except IndexError:
            raise ValueError("No cluster named {} found".format(c))
        clusters.append(cluster)

    frontend_ip = frontend['NetworkSettings']['IPAddress']

    # Configure the client credentials on all clusters
    with fab.settings(hosts=frontend_ip):
        frontend_credentials = fab_execute(fab_deploy.get_frontend_credentials).values()[0]

    for cluster in clusters:
        cluster = cluster
        cluster_name = cluster['Config']['Labels']['cluster_name']
        nodes = get_clusters(c)[cluster_name][1:]
        cluster_ip = cluster['NetworkSettings']['IPAddress']
        with fab.settings(hosts=cluster_ip):
            fab_execute(fab_deploy.generate_client_credentials, cluster_name,
                        frontend_credentials['public_key'],
                        frontend_credentials['verify_code'])
            # Get the cluster credentials and jvms list
            cluster_credentials = fab_execute(fab_deploy.get_client_credentials).values()[0]
            jvms = fab_execute(fab_deploy.get_client_jvms).values()[0]

        # Link the cluster to the frontend
        with fab.settings(hosts=frontend_ip):
            fab_execute(fab_deploy.add_cluster_to_frontend, cluster_name, nodes,
                        cluster_credentials['public_key'])
            for jvm in jvms:
                fab_execute(fab_deploy.add_jvm_to_cluster, cluster_name, jvm)

            if with_dse:
                fab_execute(fab_deploy.add_product_to_cluster, cluster_name, 'dse')

        with fab.settings(hosts=cluster_ip, user="root"):
            fab_execute(tasks.setup_client_daemon, frontend['Name'])
            fab_execute(tasks.add_or_update_host_ips, ((frontend['Name'], frontend_ip),))


def enable_dse(cluster_name, dse_url, dse_username, dse_password, dse_source_build_artifactory_url,
               dse_source_build_artifactory_username, dse_source_build_artifactory_password,
               dse_source_build_oauth_token):

    try:
        cluster = get_clusters(cluster_name, all_metadata=True)[cluster_name][0]
    except IndexError:
        raise ValueError("No cluster named {} found".format(cluster_name))

    cluster_ip = cluster['NetworkSettings']['IPAddress']
    with fab.settings(hosts=cluster_ip):
        fab_execute(fab_deploy.enable_dse, dse_url, dse_username, dse_password, dse_source_build_artifactory_url,
                    dse_source_build_artifactory_username, dse_source_build_artifactory_password,
                    dse_source_build_oauth_token)

    with fab.settings(hosts=cluster_ip, user="root"):
        fab_execute(tasks.restart_all_services)

def __update_node_ip_addresses(cluster_name, static_ips=None):
    """Update node ip addresses

    This is necessary because docker assigns new IP addresses each time a container is restarted

    if static_ips is provided, interpret as a dictionary mapping hosts to ips.
    """
    # Retrieve the current ~/.cstar_perf/cluster_config.json on node 00:
    clusters = get_clusters(cluster_name, all_metadata=True)
    cluster = clusters[cluster_name]
    current_ips = dict([(c['Name'], c['NetworkSettings']['IPAddress']) for c in cluster])
    if static_ips:
        updated_ips = static_ips
    else:
        updated_ips = current_ips
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
        cluster_config['hosts'][host]['internal_ip'] = cluster_config['hosts'][host]['external_ip'] = updated_ips[host]

    cluster_config['stress_node'] = updated_ips[node0]

    # Replace the config file onto node 0:
    with fab.settings(hosts=cluster[0]['NetworkSettings']['IPAddress']):
        def put_cluster_config():
            cfg = StringIO()
            json.dump(cluster_config, cfg, indent=2)
            fab.put(cfg, "~/.cstar_perf/cluster_config.json")
        fab_execute(put_cluster_config)

    # Update all /etc/hosts file with latest ips
    hosts = []
    clusters = get_clusters('all', all_metadata=True)
    for cluster_name in clusters.keys():
        hosts.extend(get_ips(cluster_name))
    with fab.settings(hosts=[ip for host, ip in hosts], user="root"):
        fab_execute(tasks.add_or_update_host_ips, hosts)
        fab_execute(tasks.restart_all_services)

def start(cluster_name):
    """start cluster"""
    clusters = get_clusters(cluster_name)
    cluster = clusters[cluster_name]
    for container in cluster:
        start_cmd = shlex.split("docker start {}".format(container))
        subprocess.call(start_cmd, stdout=subprocess.PIPE)
    __update_node_ip_addresses(cluster_name)

def stop(cluster_name):
    """stop cluster"""
    clusters = get_clusters(cluster_name)
    cluster = clusters[cluster_name]
    for container in cluster:
        stop_cmd = shlex.split("docker stop {}".format(container))
        subprocess.call(stop_cmd, stdout=subprocess.PIPE)

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
        if not check_cluster_exists(args.name) or args.destroy_existing:
            try:
                launch(args.num_nodes, cluster_name=args.name,
                       destroy_existing=args.destroy_existing,
                       install_tool=not args.no_install,
                       mount_host_src=args.mount, verbose=True,
                       client_double_duty=args.client_double_duty)
            except:
                destroy(args.name)
                raise
        else:
            log.error('Cannot launch cluster \'{}\' as it already exists.'.format(args.name))
            log.error('You must destroy the existing cluster, or use --destroy-existing '
                      'in your launch command')
            exit(1)            
    elif cmd == 'frontend':
        if not check_cluster_exists(args.name) or args.destroy_existing:
            try:
                launch(1, cluster_name=args.name,
                       destroy_existing=args.destroy_existing,
                       frontend=True, client_double_duty=True,
                       mount_host_src=args.mount, verbose=True)
            except:
                destroy(args.name)
                raise
        else:
            log.error('Cannot launch cluster \'{}\' as it already exists.'.format(args.name))
            log.error('You must destroy the existing cluster, or use --destroy-existing '
                      'in your launch command')
            exit(1)            
    elif cmd == 'associate':
        associate(args.frontend, args.clusters, with_dse=args.with_dse)
    elif cmd == 'start':
        start(cluster_name=args.name)
    elif cmd == 'stop':
        stop(cluster_name=args.name)
    elif cmd == 'restart':
        stop(cluster_name=args.name)
        start(cluster_name=args.name)
    elif cmd == 'destroy':
        destroy(args.cluster_regex)
    elif cmd == 'list':
        list_clusters()
    elif cmd == 'info':
        info(args.cluster_name)
    elif cmd == 'ssh':
        ssh(args.cluster_name, args.node, user=args.login_name)
    elif cmd == 'build':
        build_docker_image(force=args.force)
    elif cmd == 'enable_dse':
        enable_dse(args.frontend, args.dse_repo_url, args.dse_repo_username, args.dse_repo_password,
                   args.dse_source_build_artifactory_url, args.dse_source_build_artifactory_username,
                   args.dse_source_build_artifactory_password, args.dse_source_build_oauth_token)
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

    associate = parser_subparsers.add_parser('associate', description="Hook up one or more clusters to a cluster")
    associate.add_argument('frontend', help='The name of the frontend cluster')
    associate.add_argument('clusters', help='The names of the clusters to hook up to the frontend', nargs='+')
    associate.add_argument('--with-dse', help='Enable DSE product for this cluster', action='store_true', default=False)

    destroy = parser_subparsers.add_parser('destroy', description='Destroy clusters - specify a regex of cluster names to destroy, or specify \'all\' to destroy all clusters created')
    destroy.add_argument('cluster_regex', help='The regex of the names of clusters to destroy')

    list_clusters = parser_subparsers.add_parser('list', description='List clusters')

    info = parser_subparsers.add_parser('info', description='Print cluster information')
    info.add_argument('cluster_name', help='The name of the cluster')

    ssh = parser_subparsers.add_parser('ssh', description='SSH to cluster node')
    ssh.add_argument('cluster_name', help='The name of the cluster')
    ssh.add_argument('node', help='The node number', type=int, nargs='?', default=0)
    ssh.add_argument('-l', '--login_name', help='User to login as (default: cstar)', default='cstar')

    build = parser_subparsers.add_parser('build', description='Build the Docker image')
    build.add_argument(
        '-f', '--force', help='Force building the image by removing any existing image first', action='store_true')

    start = parser_subparsers.add_parser('start', description='Start an existing cluster')
    start.add_argument('name', help='The name of the cluster to start')

    stop = parser_subparsers.add_parser('stop', description='Stop an existing cluster')
    stop.add_argument('name', help='The name of the cluster to stop')

    restart = parser_subparsers.add_parser('restart', description='Restart an existing cluster')
    restart.add_argument('name', help='The name of the cluster to restart')

    enable_dse = parser_subparsers.add_parser('enable_dse', description="Enable DSE support")
    enable_dse.add_argument('frontend', help='The name of the frontend node')
    enable_dse.add_argument('dse_repo_url', help='DSE Tarball Repo url')
    enable_dse.add_argument('dse_repo_username', nargs='?', default=None, help='DSE Tarball Repo username')
    enable_dse.add_argument('dse_repo_password', nargs='?', default=None, help='DSE Tarball Repo password')
    enable_dse.add_argument('dse_source_build_artifactory_url', nargs='?', default=None, help='DSE Artifactory URL')
    enable_dse.add_argument('dse_source_build_artifactory_username', nargs='?', default=None, help='DSE Artifactory username')
    enable_dse.add_argument('dse_source_build_artifactory_password', nargs='?', default=None, help='DSE Artifactory password')
    enable_dse.add_argument('dse_source_build_oauth_token', nargs='?', default=None, help='DSE OAuth token for accessing GitHub Repo')

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
