"""Generic flask utilities"""
import ConfigParser
from flask import session, request, abort, make_response
from cstar_perf.frontend.lib.util import random_token
from cstar_perf.frontend import SERVER_CONFIG_PATH

def create_app_config(config_path=SERVER_CONFIG_PATH):
    config = ConfigParser.RawConfigParser()
    config.read(config_path)
    # Ensure app has secret key for signing cookies:
    if not config.has_section('server'):
        config.add_section('server')
    if not config.has_option('server','app_secret'):
        config.set('server','app_secret', random_token())
    # Ensure app has server url:
    if not config.has_option('server', 'url'):
        config.set('server', 'url', 'http://localhost:8000')
    # Ensure app has a database to connect to:
    if not config.has_option('server','cassandra_hosts'):
        config.set('server','cassandra_hosts', 'localhost')
    with open(config_path, 'w') as f:
        config.write(f)
    return config

def load_app_config(config_path=SERVER_CONFIG_PATH):
    config = ConfigParser.RawConfigParser()
    config.read(config_path)
    # Ensure app has secret key for signing cookies:
    if not config.has_section('server') or not config.has_option('server','app_secret'):
        raise AssertionError('config file ({path}) does not contain app secret'.format(SERVER_CONFIG_PATH))
    return config


def csrf_protect_app(app):
    """CSRF protection for flask apps

    Client gets token by reading meta tag set in base jinja template via {{csrf_token()}}
    All POST requests require a CSRF token in an HTTP header called X-csrf
    Tokens are valid for as long as the session exists."""

    @app.before_request
    def csrf_protect():
        if request.path == "/api/login" or session.get('bypass_csrf', False):
            # Bypass csrf protection for trusted api sessions (see /api/login_for_apps):
            return
        if request.method == "POST":
            token = session.get('_csrf_token', None)
            header = request.headers.get('X-csrf', None)
            if not token or not header or token != header:
                abort(make_response("Invalid x-csrf token", 403))

    def generate_csrf_token():
        if '_csrf_token' not in session:
            session['_csrf_token'] = random_token()
        return session['_csrf_token']

    app.jinja_env.globals['csrf_token'] = generate_csrf_token 

