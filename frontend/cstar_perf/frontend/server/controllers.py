"""Main Application Controllers"""
import functools
import hashlib

import httplib2
import os.path
import uuid
import socket
from collections import OrderedDict
import time
from datetime import datetime
from functools import partial

import zmq
import json
import ConfigParser
from flask import ( Flask, render_template, request, redirect, abort, Response,
                    jsonify, make_response, session, url_for)

from flask.ext.scrypt import generate_random_salt, generate_password_hash, check_password_hash

from apiclient.discovery import build
from oauth2client.client import ( AccessTokenRefreshError,
                                  AccessTokenCredentials,
                                  flow_from_clientsecrets,
                                  FlowExchangeError)

from app import app, app_config, db, sockets
from model import Model, UnknownUserError, UnknownTestError
from notifications import console_subscribe
from cstar_perf.frontend.lib.util import random_token
from cstar_perf.frontend.lib import screenshot, stupid_cache
from cstar_perf.frontend import SERVER_KEY_PATH
from cstar_perf.frontend.lib.crypto import APIKey

import logging
log = logging.getLogger('cstar_perf.controllers')

### Setup authentication method configured in server.conf:
try:
    authentication_type = app_config.get("server", "authentication_type")
except ConfigParser.NoOptionError:
    authentication_type = 'local'
if authentication_type == 'local':
    pass
elif authentication_type == 'google':
    ### Google+ API:
    gplus = build('plus', 'v1')
    google_client_secrets = os.path.join(os.path.expanduser("~"),'.cstar_perf','client_secrets.json')
    with open(google_client_secrets) as f:
        google_client_id = json.load(f)['web']['client_id']
else:
    raise AssertionError('Invalid authentication type configured in server.conf: {}'.format(authentication_type))

server_key = APIKey.load(SERVER_KEY_PATH)

################################################################################
#### Template functions:
################################################################################
def get_user_id():
    return session.get('user_id', None)
app.jinja_env.globals['get_user_id'] = get_user_id

def user_is_authenticated():
    return session.get('logged_in',False)
app.jinja_env.globals['user_is_authenticated'] = user_is_authenticated


################################################################################
#### Helpers
################################################################################
def user_in_role(role, user=None):
    """Find if a user is in the given role"""
    if user is None:
        user = get_user_id()
    try:
        user_roles = db.get_user_roles(get_user_id())
        if role in user_roles:
            return True
    except UnknownUserError:
        pass
    return False

def requires_auth(role):
    """Ensures the current user has the appropriate authorization before
    running the wrapped function"""
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kw):
            # Do the check:
            if user_is_authenticated():
                if user_in_role(role):
                    return function(*args, **kw)
            return make_response(render_template('access_denied.jinja2.html'), 401)
        return wrapper
    return decorator

@app.context_processor
def inject_template_variables():
    """Common variables available to all templates"""
    d = {'clusters': db.get_cluster_names(),
         'authentication_type': authentication_type,
         'google_client_id': None}
    if authentication_type == 'google':
        d['google_client_id'] = google_client_id
    return d

################################################################################
#### Page Controllers
################################################################################

@app.route('/')
def index():
    return render_template('index.jinja2.html')

def login_with_google():
    """Login via Google+"""
    log.info("Initiating login with Google+")
    code = request.data
    try:
        # Upgrade the authorization code into a credentials object
        oauth_flow = flow_from_clientsecrets(google_client_secrets, scope='')
        oauth_flow.redirect_uri = 'postmessage'
        credentials = oauth_flow.step2_exchange(code)
    except FlowExchangeError:
        return make_response(
            jsonify({'error':'Failed to upgrade the authorization code.'}), 401)

    # An ID Token is a cryptographically-signed JSON object encoded in base 64.
    # Normally, it is critical that you validate an ID Token before you use it,
    # but since you are communicating directly with Google over an
    # intermediary-free HTTPS channel and using your Client Secret to
    # authenticate yourself to Google, you can be confident that the token you
    # receive really comes from Google and is valid. If your server passes the
    # ID Token to other components of your app, it is extremely important that
    # the other components validate the token before using it.
    gplus_id = credentials.id_token['sub']
    
    stored_credentials = AccessTokenCredentials(session.get('credentials'), 
                                                request.user_agent)
    stored_gplus_id = session.get('gplus_id')
    if stored_credentials is not None and gplus_id == stored_gplus_id:
        return make_response(jsonify(
            {'success':'Current user is already connected.'}), 200)
    # Get the user's email address:
    http = httplib2.Http()
    http = credentials.authorize(http)
    # Get a list of people that this user has shared with this app.
    google_request = gplus.people().get(userId='me')
    user_obj = google_request.execute(http=http)
    email = None
    # Find the google account email:
    for e in user_obj['emails']:
        if e['type'] == 'account':
            email = e['value']
            break
    else:
        return make_response(
            jsonify({'error':'Authorization from Google failed.'}), 401)
    
    # Store the access token in the session for later use.
    session['credentials'] = credentials.access_token
    session['gplus_id'] = gplus_id
    session['logged_in'] = True
    session['user_id'] = email
    return make_response(jsonify({'success':'Successfully connected user.'}), 
                         200)

def login_with_passphrase():
    data = request.get_json(force=True)
    log.info("Initiating login with passphrase")

    try:
        if db.validate_user_passphrase(data['email'], data['passphrase']): 
            session['logged_in'] = True
            session['user_id'] = data['email']
            return make_response(jsonify({'success':'Successfully connected user.'}), 
                                 200)
    except UnknownUserError:
        pass
    return make_response(jsonify({'error':'Unauthorized - did you enter the user right user / passphrase?'}), 401)

@app.route('/login', methods=['POST'])
def login():
    """Login via digest authentication, or Google+"""
    if authentication_type == 'local':
        return login_with_passphrase()
    elif authentication_type == 'google':
        return login_with_google()
    else:
        raise AssertionError('Invalid authentication type configured in server.conf: {}'.format(authentication_type))

@app.route('/logout', methods=['GET','POST'])
def logout():
    for i in ['credentials','gplus_id','logged_in','bypass_csrf'] :
        try:
            session.pop(i)
        except KeyError:
            pass
    if request.method == "POST":
        return make_response(jsonify({'success':'Logged out.'}), 200)
    else:
        return redirect("/")


@app.route('/tests')
def tests():
    clusters = db.get_cluster_names()
    cluster_scheduled_tests = {}
    cluster_in_progress_tests = {}
    for c in clusters:
        scheduled_tests = db.get_scheduled_tests(c)
        if len(scheduled_tests) > 0:
            cluster_scheduled_tests[c] = scheduled_tests
        in_progress_tests = db.get_in_progress_tests(c)
        if len(in_progress_tests) > 0:
            cluster_in_progress_tests[c] = in_progress_tests
    completed_tests = db.get_completed_tests()
    return render_template('tests.jinja2.html', clusters=clusters, 
                           cluster_scheduled_tests=cluster_scheduled_tests, 
                           cluster_in_progress_tests=cluster_in_progress_tests,
                           completed_tests=completed_tests)

@app.route('/tests/user')
@requires_auth('user')
def my_tests():
    queued_tests = db.get_user_scheduled_tests(get_user_id())
    in_progress_tests = db.get_user_in_progress_tests(get_user_id())
    completed_tests = db.get_user_completed_tests(get_user_id())
    failed_tests = db.get_user_failed_tests(get_user_id(), 10)
    return render_template('user.jinja2.html', queued_tests=queued_tests, 
                           in_progress_tests=in_progress_tests, 
                           completed_tests=completed_tests,
                           failed_tests=failed_tests)

@app.route('/tests/id/<test_id>')
def view_test(test_id):
    try:
        test = db.get_test(test_id)
    except UnknownTestError:
        return make_response('Unknown Test {test_id}.'.format(test_id=test_id), 404)
    artifacts = db.get_test_artifacts(test_id)

    has_chart = False
    for a in artifacts:
        if a['artifact_type'] in ['failure','link']:
            # Proactively fetch non-blob artifacts:
            a['artifact'] = db.get_test_artifact_data(test_id, a['artifact_type'], a['name'])
        if a['artifact_type'] == 'stats':
            has_chart = True

    return render_template('view_test.jinja2.html', test=test, artifacts=artifacts, has_chart=has_chart)

@app.route('/tests/artifacts/<test_id>/<artifact_type>')
@app.route('/tests/artifacts/<test_id>/<artifact_type>/<artifact_name>')
def get_artifact(test_id, artifact_type, artifact_name=None):

    if artifact_type == 'graph':
        return redirect("/graph?command=one_job&stats={test_id}".format(test_id=test_id))
    elif artifact_type == 'flamegraph' and not artifact_name:
        artifacts = db.get_test_artifacts(test_id, artifact_type)
        for artifact in artifacts:
            artifact['data'] = db.get_test_artifact_data(test_id, artifact_type, artifact['name'])
        return render_template('flamegraph.jinja2.html', test_id=test_id, artifacts=artifacts)

    if not artifact_name:
        return make_response(jsonify({'error':'No artifact name provided.'}), 400)

    artifact, object_id, artifact_available = db.get_test_artifact_data(test_id, artifact_type, artifact_name)

    if artifact_name.endswith(".tar.gz"):
        mimetype = 'application/gzip'
    elif artifact_name.endswith(".json"):
        mimetype = 'application/json'
    elif artifact_name.endswith(".svg"):
        mimetype = 'image/svg+xml'
    else:
        mimetype = 'text/plain'

    if artifact is None and object_id is not None and artifact_available:
        artifact = db.generate_object_by_chunks(object_id)

    return Response(response=artifact,
                    status=200,
                    mimetype=mimetype,
                    headers={"Content-Disposition": "filename={name}".format(name=artifact_name)})

@app.route('/graph')
def graph():
    return render_template('graph.jinja2.html')


@app.route('/schedule', methods=['GET'])
@requires_auth('user')
def schedule():
    """Page to schedule a test"""
    return render_template('schedule.jinja2.html')

@app.route('/cluster/<cluster_name>')
@requires_auth('user')
def cluster(cluster_name):
    return render_template('cluster.jinja2.html',
                           cluster_name=cluster_name)

@app.route('/cluster/specs', methods=['GET'])
def cluster_specs():
    return render_template('cluster_specs.jinja2.html')

    
################################################################################
#### JSON API
################################################################################

@app.route('/api/login', methods=['GET','POST'])
def login_for_apps():
    """Login for API access only"""
    if request.method == "GET":
        session['unsigned_access_token'] = random_token()
        session['logged_in'] = False
        return jsonify({"token":session['unsigned_access_token'],
                        "signature":server_key.sign_message(session['unsigned_access_token'])})
    elif request.method == "POST":
        # Client posts it's login name and a signed token.
        data = request.get_json()
        # Verify signed token against stored public key for that name.
        pubkey = APIKey(db.get_pub_key(data['login'])['pubkey'])
        try:
            pubkey.verify_message(session['unsigned_access_token'], data['signature'])
        except Exception, e:
            session['logged_in'] = False
            del session['unsigned_access_token']
            return make_response(jsonify({'error':'Bad token signature.'}), 401)
        # Token has valid signature, grant login:
        session['user_id'] = data['login']
        session['logged_in'] = True
        # Mark this session as safe to bypass csrf protection, due to the ECDSA authentication:
        session['bypass_csrf'] = True
        return jsonify({'success':'Logged in'})


@app.route('/api/tests/schedule', methods=['POST'])
@requires_auth('user')
def schedule_test():
    """Schedule a test"""
    job = request.get_json()
    job_id = uuid.uuid1()
    job['test_id'] = str(job_id)
    job['user'] = get_user_id()
    test_series = job.get('testseries', 'no_series')
    if not test_series: 
        test_series = 'no_series'
    db.schedule_test(test_id=job_id, test_series=test_series, user=job['user'], 
                     cluster=job['cluster'], test_definition=job)
    return jsonify({'success':True, 'url':'/tests/id/{test_id}'.format(test_id=job['test_id'])})

@app.route('/api/tests/cancel', methods=['POST'])
@requires_auth('user')
def cancel_test():
    """Cancel a scheduled test"""
    test_id = request.form['test_id']
    test = db.get_test(test_id)

    # If test is scheduled, we can immediately cancel.
    # If test is already in progress, we need to mark as
    # cancel_pending to await the client to cancel the job itself.
    new_status = 'cancelled'
    if test['status'] == 'in_progress' or test['status'] == 'cancel_pending':
        new_status = 'cancel_pending'

    if user_in_role('admin'):
        db.update_test_status(test_id, new_status)
    else:
        # Check if the test is owned by the user:
        if test['user'] == get_user_id():
            db.update_test_status(test_id, new_status)
        else:
            return make_response(jsonify({'error':'Access Denied to modify test {test_id}'
                            .format(test_id=test_id)}), 401)
    return jsonify({'success':'Test cancelled'})

@app.route('/api/tests')
def get_tests():
    """Retreive all completed tests"""

    completed_tests = db.get_completed_tests()

    # Apply filters
    try:
        param_from = request.args.get('date_from', None)
        param_to = request.args.get('date_to', None)
        date_from = datetime.fromtimestamp(float(param_from)) if param_from else None
        date_to = datetime.fromtimestamp(float(param_to)) if param_to else None
    except:
        return make_response(jsonify({'error':'Invalid date parameters.'}), 400)

    if date_from:
        completed_tests = [t for t in completed_tests if t['scheduled_date'] >= date_from]
    if date_to:
        completed_tests = [t for t in completed_tests if t['scheduled_date'] <= date_to]

    tests = map(lambda t: {
        'test_id': t['test_id'],
        'href': url_for('get_test', test_id=t['test_id'])
    }, completed_tests)

    response = json.dumps(obj=tests)
    return Response(response=response,
                    status=200,
                    mimetype= 'application/json')

@app.route('/api/tests/id/<test_id>')
@requires_auth('user')
def get_test(test_id):
    """Retrieve the definition for a scheduled test"""
    try:
        test = db.get_test(test_id)
        return jsonify(test)
    except UnknownTestError:
        return make_response(jsonify({'error':'Unknown Test {test_id}.'.format(test_id=test_id)}), 404)

@app.route('/api/series/<series>/<start_timestamp>/<end_timestamp>')
def get_series( series, start_timestamp, end_timestamp):
    series = db.get_series( series, start_timestamp, end_timestamp)
    jsobj = { 'series' : series }
    if 'true' == request.args.get('pretty', 'True').lower():
        response = json.dumps(obj=jsobj, sort_keys=True, indent=4, separators=(',', ': '))
    else:
        response = json.dumps(obj=jsobj)
    return Response(response=response,
                    status=200,
                    mimetype='application/json')

def get_series_summaries_impl(series, start_timestamp, end_timestamp):
    series = db.get_series(series, start_timestamp, end_timestamp)
    summaries = []
    for test_id in series:
        status = db.get_test_status(test_id)
        if status == 'completed':
            artifact = db.get_test_artifact_data(test_id, 'stats_summary', 'stats_summary.{}.json'.format(test_id))
            if artifact and artifact[0]:
                summaries.append(json.loads(artifact[0]))
    return summaries

@app.route('/api/series/<series>/<start_timestamp>/<end_timestamp>/summaries')
def get_series_summaries(series, start_timestamp, end_timestamp):
    summaries = get_series_summaries_impl(series, start_timestamp, end_timestamp)
    # Construct the response in two passes, first sort the data points on the UUID
    # Then denormalize to arrays for each metric
    # Operation -> revision label -> uuid (for ordering) -> metrics as a bloc
    # Then do Operation -> revision label -> metrics as arrays (already sorted)
    byOperation = {}

    for summary in summaries:
        # First get everything sorted by operation, revision label (not actual revision branch/tag,sha), test id
        for stat in summary['stats']:
            operationStats = byOperation.setdefault(stat['test'], {})
            revisionStats = operationStats.setdefault(stat['label'], OrderedDict())
            revisionStats[uuid.UUID(stat['id'])] = stat
            del stat['test']
            del stat['label']

    # Now flatten the entire thing to arrays for each operation -> revision
    summaries = {}
    for operation in byOperation:
        newOperation = summaries.setdefault(operation, {})

        for revision in byOperation[operation]:
            newRevision = newOperation.setdefault(revision, {})

            for stats in byOperation[operation][revision].itervalues():
                for key, value in stats.iteritems():
                    statsArray = newRevision.setdefault(key, [])
                    if isinstance(value, basestring):
                        statsArray.append(value.split()[0])
                    else:
                        statsArray.append(value)

    # Wrapper object of facilitate adding fields later
    jsobj = { 'summaries' : summaries }

    if 'true' == request.args.get('pretty', 'True').lower():
        response = json.dumps(obj=jsobj, sort_keys=True, indent=4, separators=(',', ': '))
    else:
        response = json.dumps(obj=jsobj)
    return Response(response=response,
                    status=200,
                    mimetype= 'application/json')

def construct_series_graph_url( series, start_timestamp, end_timestamp, operation, metric ):
    redirectURL = "/graph?"
    redirectURL += "command=series"
    redirectURL += "&series={series}"
    redirectURL += "&start_timestamp={start_timestamp}"
    redirectURL += "&end_timestamp={end_timestamp}"
    redirectURL += "&metric={metric}"
    redirectURL += "&show_aggregates=false"
    redirectURL += "&operation={operation}"
    return redirectURL.format(series=series, start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                              operation=operation, metric=metric)

@app.route('/api/series/<series>/<start_timestamp>/<end_timestamp>/graph/<operation>/<metric>')
def get_series_graph( series, start_timestamp, end_timestamp, operation, metric):
    redirectURL = construct_series_graph_url(series, start_timestamp, end_timestamp, operation, metric)
    return redirect(redirectURL)

@app.route('/api/series/<series>/<start_timestamp>/<end_timestamp>/graph/<operation>/<metric>.png')
def get_series_graph_png( series, start_timestamp, end_timestamp, operation, metric):
    host = socket.gethostname()
    graphURL = "http://" + host + construct_series_graph_url( series, start_timestamp, end_timestamp, operation, metric )
    return Response(response=screenshot.get_graph_png(graphURL, x_crop=900, y_crop=650),
                    status=200,
                    mimetype='application/png')

def get_series_graph_png_cached( series, age, operation, metric, expires, invalidate):
    host = socket.gethostname()
    end_timestamp = int(time.time())
    start_timestamp = max(0, end_timestamp - int(age))
    graphURL = "http://" + host + construct_series_graph_url( series, start_timestamp, end_timestamp, operation, metric )
    def loader():
        return screenshot.get_graph_png(graphURL, x_crop=900, y_crop=650)

    cache_key = series + "/" + age + "/" + operation + "/" + metric
    return stupid_cache.stupid_cache_get("/tmp", cache_key, loader, expires, invalidate)

@app.route('/api/series/<series>/<age>/graph/cached/<operation>/<metric>.png')
def get_series_graph_png_cached_caching( series, age, operation, metric):
    return Response(response=get_series_graph_png_cached( series, age, operation, metric, 0, False),
                    status=200,
                    mimetype='application/png')

@app.route('/api/series/<series>/<age>/graph/<operation>/<metric>.png')
def get_series_graph_png_cached_invalidating( series, age, operation, metric):
    return Response(response=get_series_graph_png_cached( series, age, operation, metric, 0, True),
                    status=200,
                    mimetype='application/png')

@app.route('/api/tests/status/id/<test_id>')
@requires_auth('user')
def get_test_status(test_id):
    """Retrieve the status for a test"""
    try:
        status = db.get_test_status(test_id)
        return jsonify({'status':status})
    except UnknownTestError:
        return make_response(jsonify({'error':'Unknown Test {test_id}.'.format(test_id=test_id)}), 404)

@app.route('/api/clusters')
@requires_auth('user')
def get_clusters():
    """Retrieve information about available clusters"""
    clusters = db.get_clusters()
    return make_response(jsonify({'clusters':clusters}))

@app.route('/api/clusters/<cluster_name>')
@requires_auth('user')
def get_clusters_by_name(cluster_name):
    """Retrieve information about a cluster"""
    clusters = db.get_clusters()
    return make_response(jsonify(clusters[cluster_name]))


@app.route('/api/tests/progress/id/<test_id>', methods=['POST'])
@requires_auth('user')
def set_progress_message_on_test(test_id):
    msg = request.get_json()['progress_msg']
    db.update_test_progress_msg(test_id, msg)
    return jsonify({'status': 'ok'})


################################################################################
#### Websockets
################################################################################

@requires_auth('user')
@sockets.route('/api/console')
def console_messages(ws):
    """Receive console messages as they happen

    ZMQ message format:
     Console messages:
      console cluster_name {"job_id":"current_job_id", "msg":"message from console"}
     Control messages:
      Keep alive:
      The cluster is starting a job:
       console cluster_name {"job_id":"current_job_id", "ctl":"START"}
      The cluster finished a job:
       console cluster_name {"job_id":"current_job_id", "ctl":"DONE"}
      The cluster is not working on anything:
       console cluster_name {"ctl":"IDLE"}

    When forwarding messages to the websocket client, the "console cluster_name" 
    portion is dropped and just the JSON is sent.

    Websocket sends keepalive messages periodically:
     {"ctl":"KEEPALIVE"}

    """
    cluster_name = ws.receive()
    console_socket = console_subscribe(cluster_name)
    try:
        while True:
            try:
                data = console_socket.recv_string()
                data = data.lstrip("console {cluster_name} ".format(cluster_name=cluster_name))
                ws.send(data)
            except zmq.error.Again:
                # If we timeout from zmq, send a keep alive request to the
                # websocket client:
                ws.send('{"ctl":"KEEPALIVE"}')
                # The client websocket will send keepalive back:
                ws.receive()
            except zmq.error.ZMQError, e:

                if e.errno == zmq.POLLERR:
                    log.error(e)
                    # Interrupted zmq socket code, reinitialize:
                    # I get this when I resize my terminal.. WTF?
                    console_socket = setup_zmq()
    finally:
        log.error("Unsubscribing from zmq socket")
        console_socket.setsockopt_string(zmq.UNSUBSCRIBE, u'')
