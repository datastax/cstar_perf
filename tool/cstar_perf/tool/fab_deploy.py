"""Fabric file to setup a cluster so that fab_[cassandra|dse] can be used on it

Creates ~/.cstar_perf/cluster_config.json
Creates ~/fab directory on each node

This is currently only used by cstar_docker, but should remain useful
for other deployment types in the future.
"""

import textwrap
from fabric import api as fab
from fabric.contrib.files import append as fab_append
from fabric.tasks import execute as fab_execute
from ilogue.fexpect import expect, expecting, run
from fabric import api as fab
from fabric.tasks import execute as fab_execute
from StringIO import StringIO
import json
import re
import operator


fab.env.use_ssh_config = True
fab.env.connection_attempts = 10

def run_python_script(script):
    fab.run("rm -f ~/pyscript.py")
    fab_append("pyscript.py", textwrap.dedent(script))
    output = fab.run("python pyscript.py")
    fab.run("rm ~/pyscript.py")
    return output

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
        # I found that sometimes running the 'find' command without doing an 'ls' on the root dir would not find
        # any JDK homes when executing inside a Docker container
        fab.run('ls -la {jdk_root}'.format(jdk_root=root))
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
    
def install_cstar_perf_tool():
    fab.run("mkdir -p ~/git")
    fab.run("test ! -f ~/git/cstar_perf/tool/setup.py && git -C ~/git clone http://github.com/datastax/cstar_perf.git; true")
    with fab.settings(user='root'):
        fab.run("pip install -e /home/cstar/git/cstar_perf/tool")

    # Add all the git remotes and fetch:
    fab.run('fab -f ~/git/cstar_perf/tool/cstar_perf/tool/fab_cassandra.py -H {hosts} add_git_remotes'.format(
        hosts=",".join([fab.env.hosts] if isinstance(fab.env.hosts, basestring) else fab.env.hosts)))
    fab.run('git -C ~/fab/cassandra.git fetch --all')

def install_cstar_perf_frontend():
    """Install the frontend

    This method assumes that Cassandra is already installed and running on the frontend node
    """
    fab.run("mkdir -p ~/git")
    fab.run("test ! -f ~/git/cstar_perf/frontend/setup.py && git -C ~/git clone http://github.com/datastax/cstar_perf.git; true")
    with fab.settings(user='root'):
        fab.run("pip install -e /home/cstar/git/cstar_perf/frontend")

def copy_cluster_config(config):
    config = json.dumps(config, sort_keys=True, indent=4, separators=(',', ': '))
    config_file = StringIO(config)
    fab.run('mkdir -p ~/.cstar_perf')
    fab.put(config_file, '~/.cstar_perf/cluster_config.json')

def generate_frontend_credentials():
    """Create the server keys and application config"""

    # Save the credentials in a file, so we can use it to associate a cluster
    fab.run("cstar_perf_server --get-credentials > ~/credentials.txt")

def create_default_frontend_users():
    """Create a default admin and normal users"""

    create_users_script = """
    from cstar_perf.frontend.server.model import Model
    db = Model()
    admin = db.create_user('admin@example.com', 'Admin Full Name', ['user','admin'])
    db.set_user_passphrase('admin@example.com', 'admin')
    user = db.create_user('user@example.com', 'User Full Name', ['user'])
    db.set_user_passphrase('user@example.com', 'user')
    """
    run_python_script(create_users_script)

def add_cluster_to_frontend(cluster_name, nodes, public_key):
    """Add the cluster to the frontend configuration"""

    add_cluster_script = """
    from cstar_perf.frontend.server.model import Model
    db = Model()
    db.add_cluster('{name}', {nodes}, '{name}')
    db.add_pub_key('{name}', 'cluster', '{key}', replace=True)
    """.format(name=cluster_name, nodes=nodes, key=public_key)
    run_python_script(add_cluster_script)

def add_jvm_to_cluster(cluster_name, jvm):
    """Add a jvm to the frontend cluster configuration"""

    path = "~/fab/jvms/{jvm}".format(jvm=jvm)
    add_jvm_script = """
    from cstar_perf.frontend.server.model import Model
    db = Model()
    db.add_cluster_jvm('{name}', '{jvm}', '{path}')
    """.format(name=cluster_name, path=path, jvm=jvm)
    run_python_script(add_jvm_script)

def get_frontend_credentials():
    """Get the frontend server keys"""

    # Read the credentials file and return a dict
    output = fab.run("cat ~/credentials.txt")
    if 'Server public key' not in output:
        raise ValueError("credentials.txt doesn't contain proper keys")
    lines = output.split('\n')
    public_key = lines[1].split(': ')[1].strip()
    verify_code = lines[2].split(': ')[1].strip()

    return {'public_key': public_key, 'verify_code': verify_code}

def generate_client_credentials(cluster_name, public_key, verify_code):
    """Generate the client credentials"""

    prompts = []
    prompts += expect('Enter a name for this cluster:', cluster_name)
    prompts += expect("Input the server's public key:", public_key)
    prompts += expect("Input the server verify code: ", verify_code)

    with expecting(prompts):
        output = run('cstar_perf_client --get-credentials')

    lines = output.split('\n')
    client_public_key = [line for line in lines if line.startswith("Your public key is")][0]
    fab.run("echo '{}' > ~/credentials.txt".format(client_public_key))

def get_client_credentials():
    """Get the client server key"""

    # Read the credentials file and return a dict
    output = fab.run("cat ~/credentials.txt")
    public_key = output.split(': ')[1].strip()

    return {'public_key': public_key}

def get_client_jvms():
    """Get a list of all jvms of the client"""

    output = fab.run("ls ~/fab/jvms")
    jvms = [jvm for jvm in output.split(' ') if jvm]
    return jvms


def enable_dse(dse_repo_url, dse_repo_username=None, dse_repo_password=None, dse_source_build_artifactory_url=None,
               dse_source_build_artifactory_username=None, dse_source_build_artifactory_password=None,
               dse_source_build_oauth_token=None):
    """Enable DSE"""

    enable_dse_script = """
    import json
    from cstar_perf.tool.cluster_config import cluster_config_file, config
    config['dse_url'] = '{url}'
    username = '{username}'
    pw = '{pw}'
    dse_source_build_artifactory_url = '{dse_source_build_artifactory_url}'
    dse_source_build_artifactory_username = '{dse_source_build_artifactory_username}'
    dse_source_build_artifactory_password = '{dse_source_build_artifactory_password}'
    dse_source_build_oauth_token = '{dse_source_build_oauth_token}'

    if username:
        config['dse_username'] = username
    elif 'dse_username' in config:
        del config['dse_username']

    if pw:
        config['dse_password'] = pw
    elif 'dse_password' in config:
        del config['dse_password']

    if dse_source_build_artifactory_url:
        config['dse_source_build_artifactory_url'] = dse_source_build_artifactory_url
    elif 'dse_source_build_artifactory_url' in config:
        del config['dse_source_build_artifactory_url']

    if dse_source_build_artifactory_username:
        config['dse_source_build_artifactory_username'] = dse_source_build_artifactory_username
    elif 'dse_source_build_artifactory_username' in config:
        del config['dse_source_build_artifactory_username']

    if dse_source_build_artifactory_password:
        config['dse_source_build_artifactory_password'] = dse_source_build_artifactory_password
    elif 'dse_source_build_artifactory_password' in config:
        del config['dse_source_build_artifactory_password']

    if dse_source_build_oauth_token:
        config['dse_source_build_oauth_token'] = dse_source_build_oauth_token
    elif 'dse_source_build_oauth_token' in config:
        del config['dse_source_build_oauth_token']

    with open(cluster_config_file, 'w') as f:
        f.write(json.dumps(config, sort_keys=True, indent=4, separators=(',', ': ')))
    """.format(url=dse_repo_url, username=dse_repo_username, pw=dse_repo_password,
               dse_source_build_artifactory_url=dse_source_build_artifactory_url,
               dse_source_build_artifactory_username=dse_source_build_artifactory_username,
               dse_source_build_artifactory_password=dse_source_build_artifactory_password,
               dse_source_build_oauth_token=dse_source_build_oauth_token)
    print run_python_script(enable_dse_script)

def add_product_to_cluster(cluster_name, product):
    """Add a product to a cluster configuration"""

    add_cluster_product = """
    from cstar_perf.frontend.server.model import Model
    db = Model()
    db.add_cluster_product('{name}', '{product}')
    """.format(name=cluster_name, product=product)
    run_python_script(add_cluster_product)
