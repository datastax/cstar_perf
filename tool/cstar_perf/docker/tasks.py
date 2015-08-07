#!/usr/bin/env python2

"""
docker.tasks

Various tasks to provision the docker machines.

"""

import textwrap
from fabric import api as fab
from fabric.tasks import execute as fab_execute
from fabric.contrib.files import append as fab_append
from ilogue.fexpect import expect, expecting, run

# TODO All python scripts should be python resource file, instead of string.

def run_python_script(script):
    fab.run("rm -f ~/pyscript.py")
    fab_append("pyscript.py", textwrap.dedent(script))
    output = fab.run("python pyscript.py")
    fab.run("rm ~/pyscript.py")
    return output

def generate_frontend_credentials():
    """Create the server keys and application config"""

    # Save the credentials in a file, so we can use it to associate a cluster
    fab.run("cstar_perf_server --get-credentials > ~/credentials.txt")

def create_default_users():
    """Create a default admin and normal users"""

    create_users_script = """
    from cstar_perf.frontend.server.model import Model
    db = Model()
    admin = db.create_user('admin@admin.com', 'Admin Full Name', ['user','admin'])
    db.set_user_passphrase('admin@admin.com', 'admin')
    user = db.create_user('user@user.com', 'User Full Name', ['user'])
    db.set_user_passphrase('user@user.com', 'user')
    """
    run_python_script(create_users_script)

def add_cluster_to_frontend(cluster_name, num_nodes, public_key):
    """Add the cluster to the frontend configuration"""

    add_cluster_script = """
    from cstar_perf.frontend.server.model import Model
    db = Model()
    db.add_cluster('{name}', {num_nodes}, '{name}')
    db.add_pub_key('{name}', 'cluster', '{key}', replace=True)
    """.format(name=cluster_name, num_nodes=num_nodes, key=public_key)
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

def setup_client_daemon(node_name):
    """Add or replace cstar_perf_client config to supervisord"""

    setup_script = """
    import ConfigParser
    config = ConfigParser.RawConfigParser()
    config_path = '/supervisord.conf'
    config.read(config_path)
    if not config.has_section('program:cstar_perf_client'):
        config.add_section('program:cstar_perf_client')
    config.set('program:cstar_perf_client', 'command', 'cstar_perf_client -s ws://{}:8000/api/cluster_comms')
    config.set('program:cstar_perf_client', 'autostart', 'true')
    config.set('program:cstar_perf_client', 'autorestart', 'true')
    config.set('program:cstar_perf_client', 'redirect_stderr', 'true')
    config.set('program:cstar_perf_client', 'user', 'cstar')
    config.set('program:cstar_perf_client', 'environment', 'HOME=/home/cstar')
    config.set('program:cstar_perf_client', 'startretries', '30')
    with open(config_path, 'w') as f:
        config.write(f)
    """.format(node_name)
    run_python_script(setup_script)
    # ensure the client is restarted
    fab.run("supervisorctl -c /supervisord.conf reread")
    fab.run("supervisorctl -c /supervisord.conf stop cstar_perf_client")
    fab.run("supervisorctl -c /supervisord.conf start cstar_perf_client")

def add_or_update_host_ips(hosts):
    """Update /etc/hosts ips"""

    update_script = r"""
    import re
    host_file_path = '/etc/hosts'
    with open(host_file_path, 'r') as f:
        host_file_content = f.read()

    with open(host_file_path, 'w') as f:
        lines = host_file_content.split('\n')
        updated_hosts = []
        for line in lines:
            try:
                line_ip, line_host = re.findall(r"[\w\.]+", line)
                for host, ip in {hosts}:
                     if line_host == host:
                         line = "{{}} {{}}".format(ip, host)
                         updated_hosts.append(host)
                         break
            except ValueError:
                pass
            f.write("{{}}\n".format(line))
        new_hosts = [host for host in {hosts} if host[0] not in updated_hosts]
        for host, ip in new_hosts:
            f.write("{{}} {{}}\n".format(ip, host))
    """.format(hosts=hosts)
    run_python_script(update_script)

def restart_all_services():
    fab.run("test -f /supervisord.conf && supervisorctl -c /supervisord.conf reread")
    fab.run("test -f /supervisord.conf && supervisorctl -c /supervisord.conf restart all")
