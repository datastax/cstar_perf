#!/usr/bin/env python2

"""
docker.tasks

Various tasks related to the docker machines provisioning.

"""

from fabric import api as fab
from cstar_perf.tool import fab_deploy


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
    fab_deploy.run_python_script(setup_script)
    # ensure the client is restarted
    restart_all_services()


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
    fab_deploy.run_python_script(update_script)


def restart_all_services():
    fab.run("test -f /supervisord.conf && supervisorctl -c /supervisord.conf reread")
    fab.run("test -f /supervisord.conf && supervisorctl -c /supervisord.conf update")
    fab.run("test -f /supervisord.conf && supervisorctl -c /supervisord.conf restart all")
