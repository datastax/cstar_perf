import uuid
import json
import logging
import os

from flask import ( Flask, render_template, request, redirect, abort,
                    jsonify, make_response, session)
from flask.ext.script import Manager
from flask_sockets import Sockets

from model import Model, UnknownUserError, UnknownTestError
from util import csrf_protect_app, load_app_config
from cstar_perf.frontend.lib.util import random_token
from cstar_perf.frontend.lib.util import auth_provider_if_configured

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('geventwebsocket').setLevel(logging.DEBUG)
log = logging.getLogger('cstar_perf')


app = Flask(__name__, static_folder="../static", static_url_path="/static")
app_config = load_app_config()
app.secret_key = app_config.get('server','app_secret')
app.debug = True
manager = Manager(app)
csrf_protect_app(app)
sockets = Sockets(app)

### Cassandra backend model:
cassandra_hosts = [h.strip() for h in app_config.get('server', 'cassandra_hosts').split(",")]
from cassandra.cluster import Cluster
auth_provider = auth_provider_if_configured(app_config)
cluster = Cluster(contact_points=cassandra_hosts, auth_provider=auth_provider)

keyspace = app_config.get('server', 'cassandra_keyspace') if app_config.has_option('server', 'cassandra_keyspace') else 'cstar_perf'
db = Model(cluster=cluster, keyspace=keyspace, email_notifications=app_config.has_section('smtp'))

### Main application controllers:
import controllers

### Backend API controllers:
import cluster_api



if __name__ == "__main__":
    manager.run()
    
