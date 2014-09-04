import os, os.path
import signal
import sys
import shlex
import subprocess
import argparse

from cstar_perf.frontend.server.util import create_app_config
from cstar_perf.frontend.lib.crypto import generate_server_keys

def run_server():
    # Initialize database before gunicorn workers startup:
    from cstar_perf.frontend.server.model import Model
    db = Model()
    del db
    
    app_path = os.path.realpath(os.path.join(os.path.dirname(
        os.path.realpath(__file__)), os.path.pardir, "server"))
    os.chdir(app_path)
    
    proc = subprocess.Popen(shlex.split("gunicorn -k flask_sockets.worker --log-file=- --workers=10 app:app"))

    # Capture SIGTERM events to shutdown child gunicorn processes..
    def on_terminate(sig, frame):
        print("Killing child processes...")
        proc.terminate()
    signal.signal(signal.SIGTERM, on_terminate)

    proc.communicate()


def main():
    parser = argparse.ArgumentParser(description='cstar_perf_server')
    parser.add_argument('--get-credentials', dest='gen_credentials',
                        action='store_true', help='Get and/or create ECDSA key for signing requests.')

    args = parser.parse_args()

    if args.gen_credentials:
        generate_server_keys()
        return

    create_app_config()
    run_server()
