## Queries this app has to answer:
###
### Tests:
###  * Schedule a test to run
###    - schedule_test(test_id, user, cluster, test_definition)
###  * Retrieve a test by id
###    - get_test(test_id)
###  * Retrieve tests that have not yet run, status=new
###    - get_scheduled_tests
###  * Retrieve tests that a user has:
###    - get_scheduled_tests_by_user
###    - get_
###  * Get the next test a given cluster should run.
###    - get_next_scheduled_test
###  * Report a test that begins to be run, status=in_progress.
###    - update_test_status
###  * Report a test that failed to run properly, status=failed.
###    - update_test_status
###  * Retrieve tests that have failed, status=failed.
###    - get_failed_tests
###  * Report a test as having been run successfully, record artifacts
###    - update_test_status
###    - add_test_artificat
###  * Retrieve tests that have been completed, status=completed, 
###    - get_completed_tests
###  * Get completed test artifacts,  logs, stats, graph url.
###    - get_test_artifacts
###
### Users:
###  * Retrieve user by id (email), including set of roles (user, admin)
###
### Clusters:
###  * Create a new cluster, name, number of nodes, description
###  * Deadman switch check in - Each cluster needs to call into the
###    server on set interval, recording the time, the job the cluster
###    is currently working on, and it's status (is something else
###    using the cluster?)


import cassandra
from cassandra.cluster import Cluster, Session
import json
import uuid
import logging
import datetime
import zmq
from collections import namedtuple
import base64

Session.default_timeout = 45

from flask.ext.scrypt import generate_password_hash, generate_random_salt, check_password_hash

from flask.ext.scrypt import generate_password_hash, generate_random_salt, check_password_hash

from cstar_perf.frontend.lib.util import random_token, uuid_to_datetime, uuid_from_time
from cstar_perf.frontend.server.email_notifications import TestStatusUpdateEmail

try:
    from cassandra.util import OrderedMap
except ImportError:
    OrderedMap = None

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('cstar_perf.model')
logging.getLogger('cassandra').setLevel(logging.WARNING)


class UnknownUserError(Exception):
    pass
class UnknownTestError(Exception):
    pass
class APIKeyExistsError(Exception):
    pass
class UnknownAPIKeyError(Exception):
    pass
class NoTestsScheduledError(Exception):
    pass

TEST_STATES =  ('scheduled', 'in_progress', 'completed', 'cancel_pending', 'cancelled', 'failed')

class Model(object):

    statements = {
        'insert_test': "INSERT INTO tests (test_id, user, cluster, status, test_definition) VALUES (?, ?, ?, ?, ?);",
        'select_test': "SELECT * FROM tests WHERE test_id = ?;",
        'insert_series': "INSERT INTO test_series (series, test_id) VALUES ( ?, ?);",
        'select_series' : "SELECT test_id from test_series where series = ? AND test_id > ? AND test_id < ?",
        'select_series_list' : "SELECT DISTINCT series from test_series",
        'get_test_status': "SELECT status FROM tests WHERE test_id = ?;",
        'update_test_set_status': "UPDATE tests SET status = ? WHERE test_id = ?",
        'update_test_set_progress_msg': "UPDATE tests SET progress_msg = ? WHERE test_id = ?",
        'update_test_status_set_progress_msg': "UPDATE test_status SET progress_msg = ? WHERE status = ? and cluster = ? and test_id = ? IF EXISTS",
        'update_test_set_status_completed': "UPDATE tests SET status = ?, completed_date = ? WHERE test_id = ?",
        'insert_test_status': "INSERT INTO test_status (status, cluster, test_id, user, title) VALUES (?, ?, ?, ?, ?);",
        'select_test_status_asc': "SELECT * FROM test_status WHERE status = ? AND cluster = ? ORDER BY cluster DESC, test_id ASC LIMIT ?",
        'select_test_status_desc': "SELECT * FROM test_status WHERE status = ? AND cluster= ? ORDER BY cluster ASC, test_id DESC LIMIT ?",
        'select_test_status_by_user': "SELECT * FROM test_status WHERE status = ? AND user = ? LIMIT ?",
        'select_next_scheduled': "SELECT * FROM test_status WHERE status = 'scheduled' AND cluster = ? ORDER BY cluster DESC, test_id ASC LIMIT 1",
        'select_test_status_all': "SELECT * FROM test_status WHERE status = ? LIMIT ?",
        'delete_test_status': "DELETE FROM test_status WHERE status= ? AND cluster = ? AND test_id = ?",
        'select_clusters_name': "SELECT name from clusters;",
        'select_clusters': "SELECT name, description, jvms, nodes, additional_products FROM clusters;",
        'insert_clusters': "INSERT INTO clusters (name, nodes, description) VALUES (?, ?, ?)",
        'add_cluster_jvm': "UPDATE clusters SET jvms[?]=? WHERE name = ?",
        'add_cluster_product': "UPDATE clusters SET additional_products=additional_products + ? WHERE name = ?",
        'insert_user': "INSERT INTO users (user_id, full_name, roles) VALUES (?, ?, ?);",
        'select_user': "SELECT * FROM users WHERE user_id = ?;",
        'select_user_passphrase_hash': "SELECT hash, salt FROM user_passphrase WHERE user_id = ?;",
        'update_user_passphrase_hash': "UPDATE user_passphrase SET hash = ?, salt = ? WHERE user_id = ?",
        'select_user_roles': "SELECT roles FROM users WHERE user_id = ?;",
        'update_test_artifact': "UPDATE test_artifacts SET artifact = ?, artifact_available = ?, object_id = ? WHERE test_id = ? AND artifact_type = ? AND name = ?;",
        'select_test_artifacts_by_type': "SELECT artifact_type, name, object_id, artifact_available FROM test_artifacts WHERE test_id = ? AND artifact_type = ?",
        'select_test_artifacts_all': "SELECT artifact_type, name, object_id, artifact_available FROM test_artifacts WHERE test_id = ? ORDER BY artifact_type ASC",
        'select_test_artifact_data': "SELECT artifact, object_id, artifact_available FROM test_artifacts WHERE test_id = ? AND artifact_type = ? AND name = ?",
        'insert_chunk_object': "INSERT INTO chunk_object_storage (object_id, chunk_id, chunk_size, chunk_sha, object_chunk, total_chunks, object_size, object_sha) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        'insert_chunk_artifact_meta': "UPDATE test_artifacts SET object_id = ?, artifact_available = ? WHERE test_id = ? AND artifact_type = ? AND name = ?;",
        'select_chunk_info': "select chunk_id, chunk_sha from chunk_object_storage where object_id = ?",
        'select_base_chunk_info': "SELECT object_id, total_chunks, object_size, object_sha FROM chunk_object_storage where object_id = ? ORDER BY chunk_id ASC LIMIT 1",
        'select_chunk_data': "SELECT object_chunk FROM chunk_object_storage where object_id = ? AND chunk_id = ?",
        'insert_test_completed': "INSERT INTO tests_completed (status, completed_date, test_id, cluster, title, user) VALUES (?, ?, ?, ?, ?, ?);",
        'select_test_completed': "SELECT * FROM tests_completed LIMIT ?;",
        'select_api_pubkey': "SELECT * FROM api_pubkeys WHERE name = ? LIMIT 1",
        'insert_api_pubkey': "INSERT INTO api_pubkeys (name, user_type, pubkey) VALUES (?, ?, ?);"
    }

    def __init__(self, cluster=Cluster(['127.0.0.1'], connect_timeout=30), keyspace='cstar_perf', email_notifications=False):
        """Instantiate DB model object for interacting with the C* backend.

        cluster - Python driver object for accessing Cassandra
        keyspace - the keyspace to use
        email_notifications - if True, perform email notifications for some actions. Defaults to False.
        """
        log.info("Initializing Model...")
        self.cluster = cluster if type(cluster) == Cluster else Cluster(cluster)
        self.keyspace = keyspace
        self.email_notifications = email_notifications
        self.__shared_session = self.get_session()
        ## Prepare statements:
        self.__prepared_statements = {}
        for name, stmt in Model.statements.items():
            # log.debug("Preparing statement: {stmt}".format(stmt=stmt))
            self.__prepared_statements[name] = self.get_session().prepare(stmt)

        ### ZeroMQ publisher for announcing jobs as they come in:
        zmq_context = zmq.Context()
        self.zmq_socket = zmq_context.socket(zmq.PUSH)
        self.zmq_socket.connect("tcp://127.0.0.1:5556")

        log.info("Model initialized")

    def get_session(self, shared=True):
        try:
            if shared:
                try:
                    session = self.__shared_session
                except AttributeError:
                    session = self.cluster.connect(self.keyspace)
            else:
                session = cluster.connect(self.keyspace)
        except cassandra.InvalidRequest, e:
            # Only attempt to create the schema if we get an error that it
            # doesn't exist:
            if "Keyspace '{ks}' does not exist".format(ks=self.keyspace) in e.message:
                self.__create_schema()
            session = self.cluster.connect(self.keyspace)
        return session
                    
    def __create_schema(self, replication_factor=1):
        session = self.cluster.connect()
        log.info("Creating new schema in keyspace : {ks}".format(ks=self.keyspace))
        session.execute("""CREATE KEYSPACE {ks} WITH replication = {{'class': 'SimpleStrategy', 
                        'replication_factor': {replication_factor}}}""".format(
                            ks=self.keyspace,
                            replication_factor=replication_factor
                        ))

        session = self.cluster.connect(self.keyspace)

        # All test tests indexed by id:
        session.execute("CREATE TABLE tests (test_id timeuuid PRIMARY KEY, user text, cluster text, status text, test_definition text, completed_date timeuuid, progress_msg text);")

        # Index series by series name and then the tests by uuid
        session.execute("CREATE TABLE test_series (series text, test_id timeuuid, PRIMARY KEY (series, test_id));")

        # Tests listed by status, sorted by timestamp, in descending
        # order. Descending order because the completed status will have
        # the largest number. 'scheduled' status will want to be queried
        # in ASC order.
        session.execute("CREATE TABLE test_status (status text, test_id timeuuid, cluster text, user text, title text, progress_msg text, PRIMARY KEY (status, cluster, test_id)) WITH CLUSTERING ORDER BY (cluster ASC, test_id DESC);")
        session.execute("CREATE INDEX ON test_status (user);")
        # A denormalized copy of test_status for the completed tests.
        # This makes a reverse querying of completed tests for the
        # main page doable:
        session.execute("CREATE TABLE tests_completed (status text, completed_date timeuuid, test_id timeuuid, cluster text, title text, user text, PRIMARY KEY (status, completed_date)) WITH CLUSTERING ORDER BY (completed_date DESC)")

        # Test artifacts
        session.execute("""CREATE TABLE test_artifacts (
                                test_id timeuuid,
                                artifact_type text,
                                name text,
                                artifact blob,
                                artifact_available boolean,
                                object_id text,
                                PRIMARY KEY (test_id, artifact_type, name)
                        );""")

        session.execute("""CREATE TABLE chunk_object_storage (
                                object_id text,
                                chunk_id int,
                                chunk_size int,
                                chunk_sha text,
                                object_chunk blob,
                                total_chunks int static,
                                object_size int static,
                                object_sha text static,
                                primary key ((object_id), chunk_id)
                        );""")

        # Cluster information
        session.execute("CREATE TABLE clusters (name text PRIMARY KEY, nodes list<text>, description text, jvms map<text, text>, additional_products set<text>)")

        #Users
        session.execute("CREATE TABLE users (user_id text PRIMARY KEY, full_name text, roles set <text>);")
        session.execute("CREATE TABLE user_passphrase (user_id text PRIMARY KEY, hash text, salt text);")
        #session.execute("INSERT INTO users (user_id, full_name, roles) VALUES ('ryan@datastax.com', 'Ryan McGuire', {'user','admin'});")

        # API keys
        session.execute("CREATE TABLE api_pubkeys (name text PRIMARY KEY, user_type text, pubkey text)")


    ################################################################################
    #### Test Management:
    ################################################################################
    def schedule_test(self, test_id, test_series, user, cluster, test_definition):
        session = self.get_session()
        test_definition['test_id'] = str(test_id)
        test_json = json.dumps(test_definition)
        session.execute(self.__prepared_statements['insert_test'], (test_id, user, cluster, 'scheduled', test_json))
        session.execute(self.__prepared_statements['insert_series'], (test_series, test_id))
        session.execute(self.__prepared_statements['insert_test_status'], ('scheduled', cluster, test_id, user, test_definition['title']))
        self.zmq_socket.send_string("scheduled {cluster} {test_id}".format(**locals()))
        return test_id

    def get_test(self, test_id):
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        try:
            test = session.execute(self.__prepared_statements['select_test'], (test_id,))[0]
        except IndexError:
            raise UnknownTestError('Unknown test {test_id}'.format(test_id=test_id))
        test = self.__test_row_to_dict(test)
        return test

    def get_series(self, series, start_timestamp, end_timestamp):
        start_timestamp = int(start_timestamp)
        end_timestamp = int(end_timestamp)
        session = self.get_session()
        series = session.execute(self.__prepared_statements['select_series'], (series, uuid_from_time(start_timestamp), uuid_from_time(end_timestamp)))
        return [str(row.__dict__['test_id']) for row in series]

    def get_series_list(self):
        session = self.get_session()
        series = session.execute(self.__prepared_statements['select_series_list'])
        return [row.__dict__['series'] for row in series if row.__dict__['series'] != 'no_series']

    def get_test_status(self, test_id):
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        try:
            status = session.execute(self.__prepared_statements['get_test_status'], (test_id,))[0]
        except IndexError:
            raise UnknownTestError('Unknown test {test_id}'.format(test_id=test_id))
        return status[0]

    def update_test_progress_msg(self, test_id, progress_msg):
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        session = self.get_session()
        test = self.get_test(test_id)
        session.execute(self.__prepared_statements['update_test_set_progress_msg'], (progress_msg, test_id))
        session.execute(self.__prepared_statements['update_test_status_set_progress_msg'],
                        (progress_msg, 'in_progress', test['cluster'], test_id))

    def update_test_status(self, test_id, status):
        assert status in TEST_STATES, "{status} is not a valid test state".format(status=status)
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        test = self.get_test(test_id)
        original_status = self.get_test_status(test_id)
        # Update tests table:
        log.info('Updating test status of {test_id}. Original status: {orig}. New status: {status}.'.format(test_id=test_id, orig=original_status, status=status))
        if status == "completed":
            completed_date = uuid.uuid1()
            session.execute(self.__prepared_statements['update_test_set_status_completed'], (status, completed_date, test_id ))
            # Add denormalized copy in tests_
            session.execute(self.__prepared_statements['insert_test_completed'], ("completed", completed_date, test_id, test['cluster'], test['test_definition']['title'], test['user']))
        else:
            session.execute(self.__prepared_statements['update_test_set_status'], (status, test_id))
        # Remove the old status from test_status:
        session.execute(self.__prepared_statements['delete_test_status'], (test['status'], test['cluster'], test_id))
        # Add the new status to test_status:
        session.execute(self.__prepared_statements['insert_test_status'], (status, test['cluster'], test_id, test['user'], test['test_definition']['title']))
        self.zmq_socket.send_string("{status} {cluster} {test_id}".format(status=status, cluster=test['cluster'], test_id=test_id))
        log.info("test status of {test_id} is: {status}".format(status=status, test_id=test_id))

        # Send the user an email when the status updates:
        if self.email_notifications and status not in ('scheduled','in_progress') and status != original_status:
            TestStatusUpdateEmail([test['user']], status=status, name=test['test_definition']['title'], 
                                  test_id=test_id).send()
        return namedtuple('TestStatus', 'test_id status')(test_id, status)

    def update_test_artifact(self, test_id, artifact_type, artifact, name=None, available=True, object_id=None):
        """Update an artifact blob

        artifact can be a string or a file-like object

        test_id,artifact_type are unique in the database. If the pair already exists, it will be overridden.
        """
        if hasattr(artifact, 'read'):
            f = artifact
            pos = f.tell()
            f.seek(0)
            artifact = f.read()
            f.seek(pos)
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        if not name:
            name = "Unknown artifact"
        if hasattr(artifact, 'encode'):
            artifact = artifact.encode("hex")
        session.execute(self.__prepared_statements['update_test_artifact'], (artifact, available, object_id, test_id, artifact_type, name), timeout=60)
        return test_id

    def insert_artifact_chunk(self, object_id, chunk_id, chunk_size, chunk_sha, object_chunk, total_chunks, object_size, object_sha):
        if hasattr(object_chunk, 'read'):
            f = object_chunk
            pos = f.tell()
            f.seek(0)
            object_chunk = f.read()
            f.seek(pos)
        object_chunk = object_chunk.encode("hex")
        session = self.get_session()
        session.execute(self.__prepared_statements['insert_chunk_object'],
                        (object_id, chunk_id, chunk_size, chunk_sha, object_chunk, total_chunks, object_size, object_sha))

    def get_chunk_info(self, object_id):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_chunk_info'], (object_id, ))
        return [r.__dict__ for r in rows]

    def get_base_chunk_info(self, object_id):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_base_chunk_info'], (object_id, ))
        return [r.__dict__ for r in rows][0]

    def get_chunk_data(self, object_id, chunk_id):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_chunk_data'], (object_id, chunk_id))
        if rows:
            return rows[0].object_chunk.decode("hex")
        return ""

    def add_chunk_artifact(self, test_id, artifact_type, object_id, name, artifact_complete=False):
        session = self.get_session()
        session.execute(self.__prepared_statements['insert_chunk_artifact_meta'],
                        (object_id, artifact_complete, test_id, artifact_type, name))

    def generate_object_by_chunks(self, object_id):
        chunk_info = self.get_base_chunk_info(object_id)
        for chunk_num in range(chunk_info['total_chunks']):
            yield self.get_chunk_data(object_id, chunk_num)

    def get_test_artifact(self, test_id, artifact_type, artifact_name):
        """Retrieve one test artifact type"""
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        rows = session.execute(self.__prepared_statements['select_test_artifact'], (test_id, artifact_type, artifact_name))
        return [r.__dict__ for r in rows][0]

    def get_test_artifacts(self, test_id, artifact_type=None):
        """Retrieve all test artifacts"""
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        if artifact_type:
            rows = session.execute(self.__prepared_statements['select_test_artifacts_by_type'], (test_id, artifact_type))
        else:
            rows = session.execute(self.__prepared_statements['select_test_artifacts_all'], (test_id,))
        return [r.__dict__ for r in rows]

    def get_test_artifact_data(self, test_id, artifact_type, artifact_name):
        """Get blob data from a specific artifact"""
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        rows = session.execute(self.__prepared_statements['select_test_artifact_data'], (test_id, artifact_type, artifact_name))
        if rows:
            artifact = rows[0]
            # new chunked artifact and available
            if artifact.object_id and artifact.artifact_available:
                return ''.join(self.generate_object_by_chunks(artifact.object_id)), artifact.object_id, artifact.artifact_available
            # old style artifact
            else:
                return (artifact.artifact.decode("hex") if artifact.artifact else None), artifact.object_id, artifact.artifact_available
        return None

    ################################################################################
    ####  Retrieve tests by status:
    ################################################################################
    def get_test_status_by_cluster(self, status, cluster, direction='ASC', limit=999999999):
        session = self.get_session()
        if cluster is None:
            rows = session.execute(self.__prepared_statements['select_test_status_all'], (status, limit))
        else:
            if direction == 'ASC':
                rows = session.execute(self.__prepared_statements['select_test_status_asc'], (status, cluster, limit))
            elif direction == 'DESC':
                rows = session.execute(self.__prepared_statements['select_test_status_asc'], (status, cluster, limit))
            else:
                raise ValueError('Unknown sort direction: {direction}'.format(**locals()))
        return [self.__test_row_to_dict(r) for r in rows]

    def get_scheduled_tests(self, cluster, limit=999999999):
        return self.get_test_status_by_cluster('scheduled', cluster, 'ASC', limit)

    def get_failed_tests(self, cluster, limit=999999999):
        return self.get_test_status_by_cluster('failed', cluster, 'DESC', limit)

    def get_cancelled_tests(self, cluster, limit=999999999):
        return self.get_test_status_by_cluster('cancelled', cluster, 'DESC', limit)

    def get_next_scheduled_test(self, cluster):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_next_scheduled'], (cluster,))
        tests = [self.__test_row_to_dict(r) for r in rows]
        try:
            return tests[0]
        except IndexError:
            raise NoTestsScheduledError('No tests scheduled for {cluster}'.format(cluster=cluster))

    def get_in_progress_tests(self, cluster, limit=999999999):
        return self.get_test_status_by_cluster('in_progress', cluster, 'ASC', limit) + \
            self.get_test_status_by_cluster('cancel_pending', cluster, 'ASC', limit)

    def get_completed_tests(self, limit=999999999):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_test_completed'], (limit,))
        return [self.__test_row_to_dict(r) for r in rows]
    
    ################################################################################
    ####  Retrieve tests by user:
    ################################################################################
    def get_test_status_by_user(self, status, user, limit=999999999):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_test_status_by_user'], (status, user, limit))
        return [self.__test_row_to_dict(r) for r in rows]

    def get_user_scheduled_tests(self, user, limit=999999999):
        return self.get_test_status_by_user('scheduled', user, limit)

    def get_user_in_progress_tests(self, user, limit=999999999):
        return self.get_test_status_by_user('in_progress', user, limit)

    def get_user_completed_tests(self, user, limit=999999999):
        return self.get_test_status_by_user('completed', user, limit)

    def get_user_failed_tests(self, user, limit=999999999):
        return self.get_test_status_by_user('failed', user, limit)

    ################################################################################
    #### Cluster Management:
    ################################################################################
    def add_cluster(self, name, nodes, description):
        session = self.get_session()
        session.execute(self.__prepared_statements['insert_clusters'], (name, nodes, description))

    def get_cluster_names(self):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_clusters_name'], [])
        return [c.name for c in rows]

    def get_clusters(self):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_clusters'], [])
        clusters = {}
        for row in rows:
            jvms = row[2]

            # OrderedMap is in recent c* python driver
            if OrderedMap and isinstance(row[2], OrderedMap):
                jvms = {}
                for jvm in row[2]:
                    jvms[jvm] =  row[2][jvm]

            products = []
            if row[4]:
                for product in row[4]:
                    products.append(product)

            clusters[row[0]] = {'name': row[0],
                                'description': row[1],
                                'jvms': jvms,
                                'nodes' : row[3],
                                'additional_products' : products}
        return clusters

    def add_cluster_jvm(self, cluster, version, path):
        session = self.get_session()
        session.execute(self.__prepared_statements['add_cluster_jvm'], (version, path, cluster))

    def add_cluster_product(self, cluster, product):
        session = self.get_session()
        session.execute(self.__prepared_statements['add_cluster_product'], ([product], cluster))

    ################################################################################
    #### API Keys:
    ################################################################################
    def add_pub_key(self, name, user_type, pubkey, replace=False):
        """Add an API pubkey with the given name and user_type (cluster, user)"""
        session = self.get_session()
        if not replace:
            try:
                existing_key = self.get_pub_key(name)
                raise APIKeyExistsError('API key already exists for {name}'.format(name=name))
            except UnknownAPIKeyError:
                pass
        session.execute(self.__prepared_statements['insert_api_pubkey'], (name, user_type, pubkey))

    def get_pub_key(self, name):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_api_pubkey'], (name,))
        try:
            return rows[0].__dict__
        except IndexError:
            raise UnknownAPIKeyError('No API key named {name}'.format(name=name))

    ################################################################################
    #### User Management:
    ################################################################################
    def create_user(self, user_id, full_name, roles):
        session = self.get_session()
        session.execute(self.__prepared_statements['insert_user'], (user_id, full_name, roles))
        return user_id

    def get_user(self, user_id):
        session = self.get_session()
        try:
            return session.execute(self.__prepared_statements['select_user'], (user_id,))[0]
        except IndexError:
            raise UnknownUserError('Unknown User {user_id}'.format(user_id=user_id))

    def set_user_passphrase(self, user_id, passphrase):
        """Hash the user's passphrase and update into the database"""
        passphrase = base64.b64encode(passphrase.encode("utf-8"))
        session = self.get_session()
        salt = generate_random_salt()
        pw_hash = generate_password_hash(passphrase, salt)
        session.execute(self.__prepared_statements['update_user_passphrase_hash'], (pw_hash, salt, user_id))
        
    def get_user_passphrase_hash(self, user_id):
        session = self.get_session()
        try:
            return [s.encode("utf-8") for s in session.execute(self.__prepared_statements['select_user_passphrase_hash'], (user_id,))[0]]
        except IndexError:
            raise UnknownUserError('Unknown User {user_id}'.format(user_id=user_id))

    def validate_user_passphrase(self, user_id, passphrase):
        passphrase = base64.b64encode(passphrase.encode("utf-8"))
        res = self.get_user_passphrase_hash(user_id)
        pw_hash, salt = res
        return check_password_hash(passphrase, pw_hash, salt)
        
    def get_user_roles(self, user_id):
        session = self.get_session()
        try:
            return session.execute(self.__prepared_statements['select_user_roles'], (user_id,))[0].roles
        except IndexError:
            raise UnknownUserError('Unknown User {user_id}'.format(user_id=user_id))
    

    def __test_row_to_dict(self, row):
        test = row.__dict__
        #Deserialize test definition:
        if test.has_key('test_definition'):
            test['test_definition'] = json.loads(test['test_definition'])
        #Compute a usable date field from the test_id:
        test['scheduled_date'] = uuid_to_datetime(test['test_id'])
        log.debug(row)
        if test.has_key('completed_date') and test['completed_date'] is not None:
            test['completed_date'] = uuid_to_datetime(test['completed_date'])
        test['test_id'] = str(test['test_id'])
        return test

    def _zmq_show_jobs(self, endpoint=None, subscribe_filter='scheduled bdplab '):
        """Debug utility to show jobs via zeromq

        endpoint - the zeromq endpoint, default to the publisher endpoint of this model object"""
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        if endpoint is None:
            endpoint = self.zmq_endpoint
        socket.connect(endpoint)
        socket.setsockopt_string(zmq.SUBSCRIBE, unicode(subscribe_filter))
        while True:
            print socket.recv_string()
