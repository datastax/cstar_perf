import os, os.path
import signal
import sys
import shlex
import subprocess
import argparse
import logging

from cstar_perf.frontend.server.util import create_app_config, load_app_config
from cstar_perf.frontend.lib.crypto import get_or_generate_server_keys, SERVER_KEY_PATH
from cstar_perf.frontend.lib.util import auth_provider_if_configured
from cstar_perf.frontend.server.notifications import console_publish

log = logging.getLogger('cstar_perf.frontend.lib.server')


def run_server():
    # Initialize database before gunicorn workers startup:
    config = load_app_config()
    cassandra_hosts = [h.strip() for h in config.get('server','cassandra_hosts').split(",")]
    from cstar_perf.frontend.server.model import Model
    from cassandra.cluster import Cluster

    auth_provider = auth_provider_if_configured(config)
    cluster = Cluster(contact_points=cassandra_hosts, auth_provider=auth_provider)

    keyspace = config.get('server', 'cassandra_keyspace') if config.has_option('server', 'cassandra_keyspace') else 'cstar_perf'
    db = Model(cluster=cluster, keyspace=keyspace)
    del db

    app_path = os.path.realpath(os.path.join(os.path.dirname(
        os.path.realpath(__file__)), os.path.pardir, "server"))
    os.chdir(app_path)

    log.info('Waiting for cstar_perf_notifications startup...')
    # this will block until cstar_perf_notifications is up and running
    console_publish('dummy_cluster', {'job_id': 'startup_check', 'msg': 'checking for notification server'})

    # TODO when refactoring how the app is started, do not listen on all interfaces
    proc = subprocess.Popen(shlex.split("gunicorn -k flask_sockets.worker --bind=0.0.0.0:8000"
                                        " -t 300 --log-file=- --workers=10 app:app"))

    # Capture SIGTERM events to shutdown child gunicorn processes..
    def on_terminate(sig, frame):
        print("Killing child processes...")
        proc.terminate()
    signal.signal(signal.SIGTERM, on_terminate)

    proc.communicate()


def main():
    parser = argparse.ArgumentParser(description='cstar_perf_server')
    parser.add_argument('--get-credentials', dest='get_credentials',
                        action='store_true', help='Get and/or create ECDSA key for signing requests.')

    args = parser.parse_args()

    if not os.path.exists(SERVER_KEY_PATH):
        get_or_generate_server_keys()
        create_app_config()
        # keys are already printed, return
        if args.get_credentials:
            return

    if args.get_credentials:
        get_or_generate_server_keys()
        return

    run_server()
