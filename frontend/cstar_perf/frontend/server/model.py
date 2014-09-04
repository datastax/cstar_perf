### Queries this app has to answer:
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
from cassandra.cluster import Cluster
import json
import uuid
import logging
import datetime
import zmq
from collections import namedtuple

from cstar_perf.frontend.lib.util import random_token, uuid_to_datetime
from cstar_perf.frontend.server.email_notifications import TestStatusUpdateEmail

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


class Model(object):

    statements = {
        'insert_test': "INSERT INTO tests (test_id, user, cluster, status, test_definition) VALUES (?, ?, ?, ?, ?);",
        'select_test': "SELECT * FROM tests WHERE test_id = ?;",
        'update_test_set_status': "UPDATE tests SET status = ? WHERE test_id = ?",
        'update_test_set_status_completed': "UPDATE tests SET status = ?, completed_date = ? WHERE test_id = ?",
        'insert_test_status': "INSERT INTO test_status (status, cluster, test_id, user, title) VALUES (?, ?, ?, ?, ?);",
        'select_test_status_asc': "SELECT * FROM test_status WHERE status = ? AND cluster = ? ORDER BY cluster DESC, test_id ASC LIMIT ?",
        'select_test_status_desc': "SELECT * FROM test_status WHERE status = ? AND cluster= ? ORDER BY cluster ASC, test_id DESC LIMIT ?",
        'select_test_status_by_user': "SELECT * FROM test_status WHERE status = ? AND user = ? LIMIT ?",
        'select_next_scheduled': "SELECT * FROM test_status WHERE status = 'scheduled' AND cluster = ? ORDER BY cluster DESC, test_id ASC LIMIT 1",
        'select_test_status_all': "SELECT * FROM test_status WHERE status = ? LIMIT ?",
        'delete_test_status': "DELETE FROM test_status WHERE status= ? AND cluster = ? AND test_id = ?",
        'select_clusters_name': "SELECT name from clusters;",
        'insert_clusters': "INSERT INTO clusters (name, num_nodes, description) VALUES (?, ?, ?)",
        'insert_user': "INSERT INTO users (user_id, full_name, roles) VALUES (?, ?, ?);",
        'select_user': "SELECT * FROM users WHERE user_id = ?;",
        'select_user_roles': "SELECT roles FROM users WHERE user_id = ?;",
        'update_test_artifact': "UPDATE test_artifacts SET description = ?, artifact = ? WHERE test_id = ? AND artifact_type = ?;",
        'select_test_artifacts_by_type': "SELECT artifact_type, description FROM test_artifacts WHERE test_id = ? AND artifact_type = ?",
        'select_test_artifacts_all': "SELECT artifact_type, description FROM test_artifacts WHERE test_id = ? ORDER BY artifact_type ASC",
        'select_test_artifact_data': "SELECT artifact, description FROM test_artifacts WHERE test_id = ? AND artifact_type = ? LIMIT 1",
        'insert_test_completed': "INSERT INTO tests_completed (status, completed_date, test_id, cluster, title, user) VALUES (?, ?, ?, ?, ?, ?);",
        'select_test_completed': "SELECT * FROM tests_completed LIMIT ?;",
        'select_api_pubkey': "SELECT * FROM api_pubkeys WHERE name = ? LIMIT 1",
        'insert_api_pubkey': "INSERT INTO api_pubkeys (name, user_type, pubkey) VALUES (?, ?, ?);"
    }

    def __init__(self, cluster=Cluster(['127.0.0.1']), keyspace='cstar_perf', email_notifications=False):
        """Instantiate DB model object for interacting with the C* backend.

        cluster - Python driver object for accessing Cassandra
        keyspace - the keyspace to use
        email_notifications - if True, perform email notifications for some actions. Defaults to False.
        """
        log.info("Initializing Model...")
        self.cluster = cluster
        self.keyspace = keyspace
        self.email_notifications = email_notifications
        self.__shared_session = self.get_session()
        ## Prepare statements:
        self.__prepared_statements = {}
        for name, stmt in Model.statements.items():
            #log.debug("Preparing statement: {stmt}".format(stmt=stmt))
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
        session.execute("CREATE TABLE tests (test_id timeuuid PRIMARY KEY, user text, cluster text, status text, test_definition text, completed_date timeuuid);")
        # Tests listed by status, sorted by timestamp, in descending
        # order. Descending order because the completed status will have
        # the largest number. 'scheduled' status will want to be queried
        # in ASC order.
        session.execute("CREATE TABLE test_status (status text, test_id timeuuid, cluster text, user text, title text, PRIMARY KEY (status, cluster, test_id)) WITH CLUSTERING ORDER BY (cluster ASC, test_id DESC);")
        session.execute("CREATE INDEX ON test_status (user);")
        # A denormalized copy of test_status for the completed tests.
        # This makes a reverse querying of completed tests for the
        # main page doable:
        session.execute("CREATE TABLE tests_completed (status text, completed_date timeuuid, test_id timeuuid, cluster text, title text, user text, PRIMARY KEY (status, completed_date)) WITH CLUSTERING ORDER BY (completed_date DESC)")

        # Test artifacts
        session.execute("CREATE TABLE test_artifacts (test_id timeuuid, artifact_type text, description text, artifact blob, PRIMARY KEY (test_id, artifact_type));")

        # Cluster information
        session.execute("CREATE TABLE clusters (name text PRIMARY KEY, num_nodes int, description text)")
        
        #Users
        session.execute("CREATE TABLE users (user_id text PRIMARY KEY, full_name text, roles set <text>);")
        #session.execute("INSERT INTO users (user_id, full_name, roles) VALUES ('ryan@datastax.com', 'Ryan McGuire', {'user','admin'});")

        # API keys
        session.execute("CREATE TABLE api_pubkeys (name text PRIMARY KEY, user_type text, pubkey text)")


    ################################################################################
    #### Test Management:
    ################################################################################
    def schedule_test(self, test_id, user, cluster, test_definition):
        session = self.get_session()
        test_definition['test_id'] = str(test_id)
        test_json = json.dumps(test_definition)
        session.execute(self.__prepared_statements['insert_test'], (test_id, user, cluster, 'scheduled', test_json))
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

    def update_test_status(self, test_id, status):
        assert status in ('scheduled', 'in_progress', 'completed', 'cancelled', 'failed'), "{status} is not a valid test state".format(status=status)
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        test = self.get_test(test_id)
        # Update tests table:
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
        log.info("test status is: {status}".format(status=status))

        # Send the user an email when the status updates:
        if self.email_notifications and status not in ('scheduled','in_progress'):
            TestStatusUpdateEmail([test['user']], status=status, name=test['test_definition']['title'], 
                                  test_id=test_id).send()
        return namedtuple('TestStatus', 'test_id status')(test_id, status)

    def update_test_artifact(self, test_id, artifact_type, artifact, description=None):
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
        if not description:
            description = "Unknown artifact"
        artifact = artifact.encode("hex")
        session.execute(self.__prepared_statements['update_test_artifact'], (description, artifact, test_id, artifact_type))
        return test_id

    def get_test_artifact(self, test_id, artifact_type):
        """Retrieve one test artifact type"""
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        rows = session.execute(self.__prepared_statements['select_test_artifacts_by_type'], (test_id, artifact_type))
        return [r.__dict__ for r in rows][0]

    def get_test_artifacts(self, test_id, artifact_type=None):
        """Retrieve all test artifacts"""
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        rows = session.execute(self.__prepared_statements['select_test_artifacts_all'], (test_id,))
        return [r.__dict__ for r in rows]

    def get_test_artifact_data(self, test_id, artifact_type):
        """Get blob data from a specific artifact"""
        session = self.get_session()
        if not isinstance(test_id, uuid.UUID):
            test_id = uuid.UUID(test_id)
        rows = session.execute(self.__prepared_statements['select_test_artifact_data'], (test_id, artifact_type))
        return namedtuple('Artifact', 'artifact description')(rows[0].artifact.decode("hex"), rows[0].description)

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
        return self.get_test_status_by_cluster('in_progress', cluster, 'ASC', limit)

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
    def add_cluster(self, name, num_nodes, description):
        session = self.get_session()
        session.execute(self.__prepared_statements['insert_clusters'], (name, num_nodes, description))

    def get_cluster_names(self):
        session = self.get_session()
        rows = session.execute(self.__prepared_statements['select_clusters_name'], [])
        return [c.name for c in rows]

    ################################################################################
    #### API Keys:
    ################################################################################
    def add_pub_key(self, name, user_type, pubkey):
        """Add an API pubkey with the given name and user_type (cluster, user)"""
        session = self.get_session()
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
