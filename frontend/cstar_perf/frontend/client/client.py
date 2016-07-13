import argparse
import logging
import os
import sys
import re
import base64
import ConfigParser
import json
import datetime
import uuid
import time
import yaml
import copy
import subprocess
import pexpect
import tempfile
import tarfile
import hashlib
import shutil
import traceback
import urlparse
import threading
import psutil
import glob
from collections import namedtuple
from distutils.dir_util import mkpath
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler

import ecdsa
from daemonize import Daemonize
import fabric.api, fabric.network
from math import ceil
from websocket import WebSocket

from cstar_perf.frontend import CLIENT_CONFIG_PATH, KEEPALIVE_MARKER, EOF_MARKER
from cstar_perf.frontend.lib.crypto import APIKey, BadConfigFileException
from cstar_perf.frontend.lib.util import random_token, timeout, TimeoutError, format_bytesize, cd, generate_object_id, sha256_of_file
from cstar_perf.frontend.lib.socket_comms import Command, Response, CommandResponseBase, receive_data, UnauthenticatedError
from cstar_perf.tool.benchmark import CSTAR_PERF_LOGS_DIR
from api_client import APIClient

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('paramiko').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
log = logging.getLogger('cstar_perf.client')


class JobFailure(Exception):
    pass


class WebSocketClient(object):
    def __init__(self, ws_endpoint):
        self.ws_endpoint = ws_endpoint
        config = ConfigParser.RawConfigParser()
        config.read(CLIENT_CONFIG_PATH)
        self.__cluster_name = config.get('cluster', 'name')
        self.__client_key = APIKey.load(key_type='client')
        self.__server_key = APIKey.load(key_type='server')

        # Keep track of whether the server is connected and on the
        # same page as the client:
        self.__server_synced = False

    def connect(self):
        """Connect to the endpoint and return the opened websocket"""
        log.debug("Connecting to {ws_endpoint} ...".format(ws_endpoint=self.ws_endpoint))
        ws = self.ws = WebSocket()
        ws.connect(self.ws_endpoint)
        # Alias websocket-client's recv command to receive so that
        # it's compatible with gevent-websocket:
        ws.receive = ws.recv

        # Authenticate:
        cmd = receive_data(ws)
        self.__authenticate(cmd)
        self.__server_synced = True
        return ws

    def disconnect(self):
        self.__good_bye()

    def socket(self):
        return self.ws

    def in_sync(self):
        return self.__server_synced

    def send(self, data_or_command_response, assertions={}):
        return self.__socket_comms('send', data_or_command_response, assertions)

    def receive(self, ws_or_command_response, assertions={}):
        return self.__socket_comms('recv', ws_or_command_response, assertions)

    def respond(self, command_response, assertions={}):
        return self.__socket_comms('respond', command_response, assertions)

    def __socket_comms(self, method, obj, assertions={}):
        """Receive from a websocket, Command, or Response object.

        This wrapper is used to track websocket connection state and
        recover appropriately if the socket dies.

        method - 'send', 'recv', or 'respond'

        obj - Data to operate on, method dependent.
            send - either a text string to send on the websocket, or a prepared Command / Response object.
            recv - either a websocket object to receive from, or a Command / Response object.
            respond - a Command / Response object

        assertions - the key/value pairs of assertions, if specified,
          must exist in the Response object received. (Not applicable if
          ws_or_command is a raw websocket.)

        If there is a problem with the socket, or if any of the
        assertions fail, or if any prior receive call failed this way,
        this method will update the connection state but return
        silently. This failed state is only cleared on connect()

        """
        # Default response data if there's a problem:
        if isinstance(obj, CommandResponseBase):
            data_or_response = Response(obj.ws, {'type': 'response', 'command_id': obj['command_id']})
        else:
            data_or_response = ""

        # Try to communicate, unless we know the server has desynchronized:
        if self.__server_synced:
            try:
                if isinstance(obj, CommandResponseBase) or isinstance(obj, WebSocket):
                    data_or_response = getattr(obj, method)()
                elif method == 'send':
                    # Assume obj is a piece of data to send on the raw websocket:
                    data_or_response = self.ws.send(obj)
                if len(assertions) > 0:
                    assert isinstance(data_or_response, CommandResponseBase)
                    assertions = set(assertions.items())
                    response_set = set(data_or_response.items())
                    missing_set = assertions.difference(response_set)
                    if len(missing_set) > 0:
                        raise AssertionError(
                            "missing fields in response: {missing_set}".format(
                                missing_set=missing_set))
            except Exception, e:
                log.error("Server desynchronized: {e}".format(e=e))
                log.error(traceback.format_exc(e))
                log.warn("Can't use the websocket anymore. I'll finish this job and then disconnect.")
                self.__server_synced = False

        return data_or_response

    def __good_bye(self):
        """Tell the server we're disconnecting"""
        command = Command.new(self.socket(), action="good_bye")
        log.debug("Sending goodbye message to server..")
        command.send(await_response=False)

    def __authenticate(self, command):
        """Sign the token the server asked us to sign.
        Send it back.
        Give the server a token of our own to sign.
        Verify it."""
        assert command.get('action') == 'authenticate'
        data = {'signature': self.__client_key.sign_message(command['token']),
                'cluster':   self.__cluster_name}
        response = command.respond(**data)
        if not response.get('authenticated'):
            raise UnauthenticatedError("Our peer could not validate our signed auth token")
        # cool, the server authenticated us, now we need to
        # authenticate the server:
        token = random_token()
        cmd = Command.new(self.socket(), action='authenticate', token=token)
        response = cmd.send()
        signature = response['signature']
        # Verify the signature, raises BadSignatureError if it fails:
        try:
            self.__server_key.verify_message(token, signature)
        except:
            response.respond(message='Bad Signature of token for authentication', done=True)
            log.error('server provided bad signature for auth token')
            raise
        response.respond(authenticated=True, done=True)


class JobRunner(object):
    """Periodically requests jobs from the remote server.
    Runs them.
    Reports back."""

    def __init__(self, ws_endpoint):
        self.__ws_client = WebSocketClient(ws_endpoint)
        self.ws_endpoint = ws_endpoint

    def run(self):
        """Run a job, collect artifacts, send them to the server"""

        try:
            os.makedirs(os.path.join(os.path.expanduser("~"), '.cstar_perf', 'jobs'))
        except OSError:
            pass

        self.__ws_client.connect()

        # Find old jobs, find their status, update the server
        self.recover_jobs()

        # Get work task loop:
        while True:
            # - Get next job.
            log.debug("Asking for a new job...")
            job = self.__get_work()
            log.debug("Got job: {job}".format(job=job))

            status_to_submit = "completed"
            message = None
            stacktrace = None
            try:
                self.perform_job(job)
            except Exception, e:
                message = e.message
                stacktrace = traceback.format_exc(e)
                log.error(stacktrace)
                status_to_submit = "failed"

            # check if the server desynchronized (socket died or got
            # invalid response):
            if not self.__ws_client.in_sync():
                # Break out of this job loop. This will force a
                # disconnect and the job data will be resent upon
                # reconnection (self.recover_jobs())
                log.error("Disconnecting from server due to websocket desynchronization.")
                job_dir = os.path.join(os.path.expanduser("~"), '.cstar_perf', 'jobs', job['test_id'])
                log.error("Last job directory: {job_dir}".format(job_dir=job_dir))
                # Save any failure messages to the job directory to
                # send to the server later:
                with open(os.path.join(job_dir, 'failure.json'), 'w') as f:
                    json.dump({"message":message, "stacktrace":stacktrace}, f)
                break
            job_id = job['test_id']
            if status_to_submit == 'completed' and JobStatusRetriever.get_job_status(test_id=job_id, api_endpoint_url=urlparse.urlparse(self.ws_endpoint).netloc) == 'cancel_pending':
                log.info('Job {job_id} was previously pending a cancel - setting job status to cancelled in the db'.format(job_id=job_id))
                self.__job_done(job_id, status='cancelled')
            else:
                self.__job_done(job_id, status=status_to_submit, message=message, stacktrace=stacktrace)
            
    def perform_job(self, job):
        """Perform a job the server gave us, stream output and artifacts to the given websocket."""
        job = copy.deepcopy(job['test_definition'])
        # Cleanup the job structure according to what stress_compare needs:
        for operation in job['operations']:
            operation['type'] = operation['operation']
            del operation['operation']

        job_dir = os.path.join(os.path.expanduser('~'),'.cstar_perf','jobs',job['test_id'])
        mkpath(job_dir)
        stats_path = os.path.join(job_dir,'stats.{test_id}.json'.format(test_id=job['test_id']))
        summary_path = os.path.join(job_dir,'stats_summary.{test_id}.json'.format(test_id=job['test_id']))
        stress_log_path = os.path.join(job_dir,'stress_compare.{test_id}.log'.format(test_id=job['test_id']))

        stress_json = json.dumps(dict(revisions=job['revisions'],
                                      operations=job['operations'],
                                      title=job['title'],
                                      leave_data=job.get('leave_data', False),
                                      log=stats_path))

        # Create a temporary location to store the stress_compare json file:
        stress_json_path = os.path.join(job_dir, 'test.{test_id}.json'.format(test_id=job['test_id']))
        with open(stress_json_path, 'w') as f:
            f.write(stress_json)

        # Inform the server we will be streaming the console output to them:
        command = Command.new(self.__ws_client.socket(), action='stream', test_id=job['test_id'],
                              kind='console', name="stress_compare.{test_id}.log".format(test_id=job['test_id']),
                              eof=EOF_MARKER, keepalive=KEEPALIVE_MARKER)
        response = self.__ws_client.send(command, assertions={'message':'ready'})

        # Start a status checking thread.
        # If a user cancel's the job after it's marked in_progress, we
        # need to periodically check for that state change and kill
        # our test:
        cancel_checker = JobCancellationTracker(urlparse.urlparse(self.ws_endpoint).netloc, job['test_id'])
        cancel_checker.start()

        # stats file observer
        # looks for changes to update server with status progress message
        observer = Observer()
        observer.schedule(UpdateServerProgressMessageHandler(job, urlparse.urlparse(self.ws_endpoint).netloc),
                          os.path.join(os.path.expanduser("~"), '.cstar_perf', 'jobs'),
                          recursive=True)
        observer.start()

        # Run stress_compare in a separate process, collecting the
        # output as an artifact:
        try:
            # Run stress_compare with pexpect. subprocess.Popen didn't
            # work due to some kind of tty issue when invoking
            # nodetool.
            stress_proc = pexpect.spawn('cstar_perf_stress {stress_json_path}'.format(stress_json_path=stress_json_path), timeout=None)
            with open(stress_log_path, 'w') as stress_log:
                while True:
                    try:
                        with timeout(25):
                            line = stress_proc.readline()
                            if line == '':
                                break
                            stress_log.write(line)
                            sys.stdout.write(line)
                            self.__ws_client.send(base64.b64encode(line))
                    except TimeoutError:
                        self.__ws_client.send(base64.b64encode(KEEPALIVE_MARKER))
        finally:
            cancel_checker.stop()
            observer.stop()
            self.__ws_client.send(base64.b64encode(EOF_MARKER))

        response = self.__ws_client.receive(response, assertions={'message': 'stream_received', 'done': True})

        # Find the log tarball for each revision by introspecting the stats json:
        system_logs = []
        flamegraph_logs = []
        yourkit_logs = []
        log_dir = CSTAR_PERF_LOGS_DIR
        flamegraph_dir = os.path.join(os.path.expanduser("~"), '.cstar_perf', 'flamegraph')
        yourkit_dir = os.path.join(os.path.expanduser("~"), '.cstar_perf', 'yourkit')
        operation_artifacts_dir = os.path.join(os.path.expanduser("~"), '.cstar_perf', 'operation_artifacts')
        #Create a stats summary file without voluminous interval data
        if os.path.isfile(stats_path):
            with open(stats_path) as stats:
                stats = json.loads(stats.read())
                for rev in stats['revisions']:
                    last_log_rev_id = rev.get('last_log')
                    if last_log_rev_id:
                        system_logs.append(os.path.join(log_dir, "{name}.tar.gz".format(name=last_log_rev_id)))
                        fg_path = os.path.join(flamegraph_dir, "{name}.tar.gz".format(name=last_log_rev_id))
                        yourkit_path = os.path.join(yourkit_dir, "{name}.tar.gz".format(name=last_log_rev_id))
                        if os.path.exists(fg_path):
                            flamegraph_logs.append(fg_path)
                        if os.path.exists(yourkit_path):
                            yourkit_logs.append(yourkit_path)
                with open(summary_path, 'w') as summary:
                    hadStats = False
                    for op in stats['stats']:
                        if op['type'] == 'stress':
                            try:
                                del op['intervals']
                                hadStats = True
                            except KeyError:
                                pass
                        try:
                            del op['output']
                        except KeyError:
                            pass
                    if hadStats:
                        json.dump(obj=stats, fp=summary, sort_keys=True, indent=4, separators=(',', ': '))

        # Make a new tarball containing all the revision logs:
        tmptardir = tempfile.mkdtemp()
        try:
            startup_log_tarball = self._maybe_get_startup_log_tarball(job['test_id'], log_dir)
            if startup_log_tarball:
                system_logs.append(startup_log_tarball)
            job_log_dir = os.path.join(tmptardir, 'cassandra_logs.{test_id}'.format(test_id=job['test_id']))
            os.mkdir(job_log_dir)
            for x, syslog in enumerate(system_logs, 1):
                with tarfile.open(syslog) as tar:
                    tar.extractall(job_log_dir)
                    os.rename(os.path.join(job_log_dir, tar.getnames()[0]), os.path.join(job_log_dir, 'revision_{x:02d}'.format(x=x)))
            system_logs_path = os.path.join(job_dir, 'cassandra_logs.{test_id}.tar.gz'.format(test_id=job['test_id']))
            with tarfile.open(system_logs_path, 'w:gz') as tar:
                with cd(tmptardir):
                    tar.add('cassandra_logs.{test_id}'.format(test_id=job['test_id']))
            assert os.path.exists(system_logs_path)
        finally:
            shutil.rmtree(tmptardir)

        # Make a new tarball containing all the flamegraph and data
        if flamegraph_logs:
            tmptardir = tempfile.mkdtemp()
            try:
                flamegraph_tmp_dir = os.path.join(tmptardir, 'flamegraph_logs.{test_id}'.format(test_id=job['test_id']))
                os.mkdir(flamegraph_tmp_dir)
                for x, flamegraph in enumerate(flamegraph_logs, 1):
                    with tarfile.open(flamegraph) as tar:
                        tar.extractall(flamegraph_tmp_dir)
                        tmp_dir = os.path.join(flamegraph_tmp_dir, tar.getnames()[0])

                        # Copy all flamegraph as artifacts
                        for node_dir in os.listdir(tmp_dir):
                            glob_match = os.path.join(os.path.join(tmp_dir, node_dir), '*.svg')
                            graphs = glob.glob(glob_match)
                            for graph in graphs:
                                graph_name = os.path.basename(graph).replace(
                                    'flamegraph_', 'flamegraph_{}_{}_'.format(job['test_id'], node_dir))
                                graph_dst_filename = os.path.join(job_dir, graph_name)
                                shutil.copyfile(graph, graph_dst_filename)

                        os.rename(tmp_dir, os.path.join(flamegraph_tmp_dir, 'revision_{x:02d}'.format(x=x)))

                flamegraph_job_path = os.path.join(job_dir, 'flamegraph_logs.{test_id}.tar.gz'.format(test_id=job['test_id']))
                with tarfile.open(flamegraph_job_path, 'w:gz') as tar:
                    with cd(tmptardir):
                        tar.add('flamegraph_logs.{test_id}'.format(test_id=job['test_id']))
                assert os.path.exists(flamegraph_job_path)
            finally:
                shutil.rmtree(tmptardir)

        # Make a new tarball containing all the flamegraph and data
        if yourkit_logs:
            tmptardir = tempfile.mkdtemp()
            try:
                yourkit_tmp_dir = os.path.join(tmptardir, 'yourkit.{test_id}'.format(test_id=job['test_id']))
                os.mkdir(yourkit_tmp_dir)
                for x, yourkit in enumerate(yourkit_logs, 1):
                    with tarfile.open(yourkit) as tar:
                        tar.extractall(yourkit_tmp_dir)
                        tmp_dir = os.path.join(yourkit_tmp_dir, tar.getnames()[0])
                        os.rename(tmp_dir, os.path.join(yourkit_tmp_dir, 'revision_{x:02d}'.format(x=x)))

                yourkit_job_path = os.path.join(job_dir, 'yourkit.{test_id}.tar.gz'.format(test_id=job['test_id']))
                with tarfile.open(yourkit_job_path, 'w:gz') as tar:
                    with cd(tmptardir):
                        tar.add('yourkit.{test_id}'.format(test_id=job['test_id']))
                assert os.path.exists(yourkit_job_path)
            finally:
                shutil.rmtree(tmptardir)

        if os.path.exists(operation_artifacts_dir):
            for file_ in os.listdir(operation_artifacts_dir):
                filepath = os.path.join(operation_artifacts_dir, file_)
                shutil.move(filepath, job_dir)
            shutil.rmtree(operation_artifacts_dir)


        ## Stream artifacts
        ## Write final job status to 0.job_status file
        final_status = 'local_complete'
        try:
            # Stream artifacts:
            self.stream_artifacts(job['test_id'])
            if self.__ws_client.in_sync():
                final_status = 'server_complete'

            # Spot check stats to ensure it has the data it should
            # contain. Raises JobFailure if something's amiss.
            try:
                self.__spot_check_stats(job, stats_path)
            except JobFailure, e:
                if final_status == 'server_complete':
                    final_status = 'server_fail'
                else:
                    final_status = 'local_fail'
                raise
        finally:
            with open(os.path.join(job_dir, '0.job_status'), 'w') as f:
                f.write(final_status)

    def _maybe_get_startup_log_tarball(self, job_id, log_dir):
        """
        In case something went wrong during startup of C*, we will basically only have a single tarball
        with the startup log.
        :param job_id: The id of the cstar_perf job
        :param log_dir: The log directory
        :return: The full path to the startup log tarball if it exists, otherwise None.
        """
        startup_logs_tarball = os.path.join(log_dir, "{name}.tar.gz".format(name=job_id))
        if os.path.exists(startup_logs_tarball):
            log.info('Found tarball with startup log at {loc}'.format(loc=startup_logs_tarball))
            return startup_logs_tarball
        return None

    def stream_artifacts(self, job_id):
        """Stream all job artifacts

        Artifacts this looks for:
          console     - stress_compare console logs
            We already stream console logs during the job itself, but
            these can get interrupted, so, better to send it again.
          stats       - stress statistics (intervals, aggregates) JSON
          stats_summary - stress statistics with only aggregates
          system_logs - cassandra logs
          flamegraph_logs - data logs and graphs
          flamegraph - all flamegraphs
          yourkit - yourkit data

        returns a namedtuple of sent, failed to transmit, or missing artifacts.
        """
        streamed = []
        failed = []
        missing = []
        job_dir = os.path.join(os.path.expanduser("~"), ".cstar_perf", "jobs", job_id)

        def stream(kind, name_pattern, binary):
            name = name_pattern.format(job_id=job_id)
            path = os.path.join(job_dir, name)
            artifacts = [path]

            if '*' in name_pattern:
                artifacts = []
                glob_match = os.path.join(job_dir, name)
                graphs = glob.glob(glob_match)
                artifacts = graphs

            for artifact in artifacts:
                if os.path.isfile(artifact):
                    self.stream_artifact_in_chunks(job_id, kind, os.path.basename(artifact), artifact, binary)
                    if self.__ws_client.in_sync():
                        streamed.append(kind)
                    else:
                        failed.append(kind)
                else:
                    missing.append(kind)

        for kind, pattern, binary in (
                ('console', 'stress_compare.{job_id}.log', False),
                ('stats', 'stats.{job_id}.json', False),
                ('stats_summary', 'stats_summary.{job_id}.json', False),
                ('system_logs', 'cassandra_logs.{job_id}.tar.gz', True),
                ('flamegraph_logs', 'flamegraph_logs.{job_id}.tar.gz', True),
                ('flamegraph', 'flamegraph_{job_id}*.svg', False),
                ('yourkit', 'yourkit.{job_id}.tar.gz', True),
                ('operations', 'operation*', False),
        ):
            stream(kind, pattern, binary)

        return namedtuple('StreamedArtifacts', 'streamed failed missing')(streamed, failed, missing)

    def stream_artifact(self, job_id, kind, name, path, binary=False):
        """Stream job artifact to server"""
        # Inform the server we will be streaming an artifact:
        command = Command.new(self.__ws_client.socket(), action='stream', test_id=job_id,
                              kind=kind, name=name, eof=EOF_MARKER, keepalive=KEEPALIVE_MARKER, binary=binary)
        response = self.__ws_client.send(command, assertions={'message': 'ready'})

        fsize = format_bytesize(os.stat(path).st_size)
        with open(path) as f:
            log.info('Streaming {name} - {path} ({fsize})'.format(name=name, path=path, fsize=fsize))
            while True:
                data = f.read(512)
                if data == '':
                    break
                data = base64.b64encode(data)
                self.__ws_client.send(data)
        self.__ws_client.send(base64.b64encode(EOF_MARKER))
        self.__ws_client.receive(response, assertions={'message': 'stream_received', 'done': True})

    @staticmethod
    def _get_chunks(file_size, chunk_size=10485760):
        chunk_num = 0
        chunk_start = 0
        while chunk_start + chunk_size < file_size:
            yield(chunk_start, chunk_size, chunk_num)
            chunk_start += chunk_size
            chunk_num += 1

        final_chunk_size = file_size - chunk_start
        yield(chunk_start, final_chunk_size, chunk_num)

    def stream_artifact_in_chunks(self, job_id, kind, name, path, binary=False):
        """Stream job artifact to server in chunks"""

        file_size = os.path.getsize(path)
        object_id = generate_object_id(job_id, kind, name)
        object_sha = sha256_of_file(path)
        num_chunks = len(list(self._get_chunks(file_size)))

        query = Command.new(self.__ws_client.socket(), action='chunk-stream-query', object_id=object_id)
        query_result = self.__ws_client.send(query, assertions={'message': 'ok'})
        existing_chunk_shas = {}
        if 'stored_chunk_shas' in query_result and query_result['stored_chunk_shas'] != '':
            existing_chunk_shas = dict(item.split(":") for item in query_result['stored_chunk_shas'].split(","))
            log.debug("found existing stored chunks: {}".format(existing_chunk_shas))

        matching_uploaded_chunks = 0
        try:
            for chunk_start, chunk_size, chunk_id in self._get_chunks(file_size):
                # skip if server already has chunk stored
                if str(chunk_id) in existing_chunk_shas:
                    log.info("chunk {} already exists on server skipping upload".format(chunk_id))
                    continue

                log.info("sending artifact[{}][{}] chunk: {}".format(name, object_id, chunk_id))

                command = Command.new(self.__ws_client.socket(), action='chunk-stream', test_id=job_id, file_size=file_size,
                                      num_of_chunks=num_chunks, chunk_id=chunk_id, object_id=object_id,
                                      object_sha=object_sha, chunk_size=chunk_size,
                                      kind=kind, name=name, eof=EOF_MARKER, keepalive=KEEPALIVE_MARKER, binary=binary)
                response = self.__ws_client.send(command, assertions={'message': 'ready'})

                chunk_sha = hashlib.sha256()
                with open(path) as fh:
                    fh.seek(chunk_start, os.SEEK_SET)
                    while fh.tell() < (chunk_start + chunk_size):
                        byte_size = 512 if fh.tell() + 512 < (chunk_start + chunk_size) else (chunk_start + chunk_size) - fh.tell()
                        data = fh.read(byte_size)
                        chunk_sha.update(data)
                        data = base64.b64encode(data)
                        self.__ws_client.send(data)
                    self.__ws_client.send(base64.b64encode(EOF_MARKER))
                    response = self.__ws_client.receive(response, assertions={'message': 'chunk_received', 'done': True})

                    if 'chunk_sha' in response and chunk_sha.hexdigest() == response['chunk_sha']:
                        matching_uploaded_chunks += 1
                    else:
                        log.error('chunk upload failed: response[{}], objectid: [{}], chunkid: [{}], totalchunks: [{}], name: [{}]'
                                  .format(response, object_id, chunk_id, num_chunks, name))
        finally:
            uploaded_successfully = matching_uploaded_chunks == num_chunks
            log.info("UPLOADED SUCCESS: {}".format(uploaded_successfully))
            self.__ws_client.send(Command.new(self.__ws_client.socket(), action='chunk-stream-complete', successful=uploaded_successfully,
                                  test_id=job_id, object_id=object_id, kind=kind, name=name),
                      assertions={'message': 'ok'})

    def recover_jobs(self):
        """Find old jobs that are still on this machine and update the server on their state.

        This is used to cleanup after network or procedural failures.
        """
        # Iterate over every directory in ~/.cstar_perf/jobs
        log.info("Looking for old jobs that did not get sent to the server ...")
        for job_dir in os.listdir(os.path.join(os.path.expanduser("~"), '.cstar_perf', 'jobs')):
            job_dir = os.path.join(os.path.expanduser("~"), '.cstar_perf', 'jobs', job_dir)
            if not os.path.isdir(job_dir):
                continue
            job_id = os.path.split(job_dir)[-1]
            # Look for a file called 0.job_status which contains
            # hints as to the last step taken.
            test_status = 'local_fail'
            if os.path.isfile(os.path.join(job_dir, '0.job_status')):
                with open(os.path.join(job_dir, '0.job_status')) as f:
                    test_status = f.read().strip()
            if test_status.startswith('server'):
                # We uploaded all artifacts and the server has
                # marked with the final status. We can just delete
                # this directory:
                ### shutil.rmtree(job_dir)
                pass
            elif test_status == 'local_complete':
                # This job completed successfully, but it has not
                # been uploaded to the server, or there was a
                # problem uploading to the server. We should try
                # again:
                self.stream_artifacts(job_id)
                if self.__ws_client.in_sync():
                    # in case the job is in cancel_pending status and artifacts were stored, we need to set the job status to
                    # 'cancelled' in the db
                    if JobStatusRetriever.get_job_status(test_id=job_id, api_endpoint_url=urlparse.urlparse(self.ws_endpoint).netloc) == 'cancel_pending':
                        log.info('Job {job_id} was previously pending a cancel - setting job status to cancelled in the db'.format(job_id=job_id))
                        self.__job_done(job_id, status='cancelled')
                    else:
                        self.__job_done(job_id, status='completed')
                    with open(os.path.join(job_dir, '0.job_status'), 'w') as f:
                        f.write('server_complete')                    
            else:
                # local_fail
                # This test did not get to local_complete status, so
                # we should upload whatever artifacts we have, and
                # tell the server the test failed
                self.stream_artifacts(job_id)
                if self.__ws_client.in_sync():
                    failure_json = os.path.join(job_dir, 'failure.json')
                    failures = {}
                    if os.path.exists(failure_json):
                        with open(failure_json) as f:
                            failures = json.load(f)
                    self.__job_done(job_id, status='failed', **failures)
                    with open(os.path.join(job_dir, '0.job_status'), 'w') as f:
                        f.write('server_fail')

            if not self.__ws_client.in_sync():
                raise JobFailure("Server desynchronized while we were trying to send an old job. We'll try again later.")

    def __spot_check_stats(self, job, stats_path):
        """Spot check stats to ensure it has the data it should contain"""
        try:
            with open(stats_path) as stats:
                stats = json.loads(stats.read())
                for op_num, op in enumerate(job['operations']):
                    assert stats['stats'][op_num]['type'] == op['type']
                    if op['type'] in ('stress', 'nodetool'):
                        assert stats['stats'][op_num]['command'].startswith(op['command'])
                    if op['type'] == 'stress':
                        assert len(stats['stats'][op_num]['intervals']) > 0
        except Exception, e:
            message = e.message
            stacktrace = traceback.format_exc(e)
            raise JobFailure("job stats is incomplete. message={message}\n{stacktrace}".format(message=message,
                                                                                               stacktrace=stacktrace))

    def __get_work(self):
        """Ask the server for work"""
        command = Command.new(self.__ws_client.socket(), action='get_work')
        response = command.send()
        while True:
            # We either got a job, or we received a wait request:
            if response.get('action') == 'wait':
                response = response.receive()
                continue
            elif response.has_key('test'):
                break
            else:
                raise AssertionError(
                    'Response was neither a wait action, nor contained '
                    'any test for us to run: {response}'.format(response=response))
        job = response['test']
        test_id = job['test_id']
        response = response.respond(test_id=test_id, status='prepared')
        assert response['status'] == 'in_progress'
        return job

    def __job_done(self, job_id, status='completed', message=None, stacktrace=None):
        """Tell the server we're done with a job, and give it the test artifacts"""
        ##{type:'command', command_id:'llll', action:'test_done', test_id:'xxxxxxx'}
        command = Command.new(self.__ws_client.socket(), action="test_done", test_id=job_id, status=status)
        if message is not None:
            command['message'] = message
        if stacktrace is not None:
            command['stacktrace'] = stacktrace
        log.debug("Sending job completion message for {test_id} ...".format(test_id=job_id))
        response = command.send()
        ##{type:'response', command_id:'llll', test_id:'xxxxxx', message='test_update', done:true}
        assert response['test_id'] == job_id
        assert response['message'] == 'test_update'
        assert response['done'] == True
        log.debug("Server confirms job {test_id} is complete.".format(test_id=job_id))


class UpdateServerProgressMessageHandler(RegexMatchingEventHandler):
    def __init__(self, job, api_endpoint_url):
        super(UpdateServerProgressMessageHandler, self).__init__(regexes=[r'^.*stats\..*\.json$'])
        self._api_endpoint_url = api_endpoint_url
        self._job = job

    def on_modified(self, event):
        self.__tell_server(event)

    def on_created(self, event):
        self.__tell_server(event)

    def __tell_server(self, event):
        with open(event.src_path) as fh:
            stats_json = json.loads(fh.read())
        last_stat = stats_json['stats'][-1]
        total_ops = len(self._job['operations']) * len(self._job['revisions'])

        msg = "Last Op Completed: {}:{}, finished {} of {} total ops ({})".format(
            last_stat['revision'], last_stat['type'], len(stats_json['stats']), total_ops, str(datetime.datetime.now())
        )
        api_client = APIClient(self._api_endpoint_url)
        api_client.post('/tests/progress/id/{}'.format(self._job['test_id']), data=json.dumps({'progress_msg': msg}))


class JobStatusRetriever(object):

    @staticmethod
    def get_job_status(test_id, api_endpoint_url):
        api_client = APIClient(api_endpoint_url)
        try:
            status = api_client.get('/tests/status/id/' + test_id)
        except Exception as e:
            log.error(e.message)
            status = None
        log.debug('JobStatusRetriever -- status of test_id {test_id} is: {s}'.format(s=status, test_id=test_id))
        return status.get('status') if status else None


class JobCancellationTracker(threading.Thread):
    """Thread to poll test status changes on the server and kill jobs if requested"""
    def __init__(self, server, test_id, check_interval=60):
        self.test_id = test_id
        self.stop_requested = False
        self.api_client = APIClient(server)
        self.check_interval = check_interval
        threading.Thread.__init__(self)
        log.info("Starting to watch for job status changes on the server for: {}".format(self.test_id))

    def run(self):
        while not self.stop_requested:
            time.sleep(self.check_interval)
            # Check job status:
            try:
                status = self.api_client.get('/tests/status/id/' + self.test_id)
            except Exception as e:
                log.error(e.message)
                status = None
            log.debug('JobCancellationTracker -- status of test_id {test_id} is: {s}'.format(s=status, test_id=self.test_id))
            if status and status.get('status', None) in ('cancelled', 'cancel_pending'):
                self.kill_jobs()

    def stop(self):
        self.stop_requested = True

    def kill_jobs(self):
        """Kill cstar_perf_stress and cassandra-stress"""
        for proc in psutil.process_iter():
            if proc.name().startswith("cstar_perf_stre"):
                log.info("Terminating cstar_perf_stress - pid:{}".format(proc.pid))
                proc.terminate()
            if proc.name() == "java":
                if "org.apache.cassandra.stress.Stress" in " ".join(proc.cmdline()):
                    log.info("Killing cassandra-stress - pid:{}".format(proc.pid))
                    proc.kill()


def create_credentials():
    """Create ecdsa keypair for authenticating with server. Save these to
    a config file in the home directory."""
    print("Config file is : {config_path}".format(config_path=CLIENT_CONFIG_PATH))
    # Check for cluster name:
    config = ConfigParser.RawConfigParser()
    config.read(CLIENT_CONFIG_PATH)
    if not config.has_section("cluster"):
        config.add_section("cluster")
    if config.has_option('cluster', 'name'):
        cluster_name = config.get('cluster', 'name')
    else:
        while True:
            cluster_name = raw_input('Enter a name for this cluster: ')
            if not re.match(r'^[a-zA-Z0-9_-]+$', cluster_name):
                print("Cluster name must be of the characters a-z, A-Z, 0-9, _ and -")
                continue
            break
        config.set('cluster', 'name', cluster_name)
        with open(CLIENT_CONFIG_PATH, "wb") as f:
            config.write(f)
        os.chmod(CLIENT_CONFIG_PATH, 0600)
    print("Cluster name is: {cluster_name}\n".format(cluster_name=cluster_name))

    # Check for existing client key:
    try:
        # Load existing config:
        apikey = APIKey.load()
    except BadConfigFileException:
        apikey = APIKey.new()
        apikey.save()

    print("Your public key is: {key}\n".format(key=apikey.get_pub_key()))

    # Check for existing server key:    
    try:
        # Load existing config:
        apikey = APIKey.load(key_type='server')
    except BadConfigFileException:
        server_key = raw_input("Input the server's public key:")
        try:
            apikey = APIKey(server_key)
        except:
            print("Invalid server key, does not decode to a valid ecdsa key.")
            exit(1)
        # Verify server key:
        verify_code = raw_input("Input the server verify code: ")
        token, sig = base64.decodestring(verify_code).split("|")
        apikey.verify_message(token, sig)
        apikey.save(key_type='server')
    
    print("Server public key is: {key}".format(key=apikey.get_pub_key()))


def main():
    parser = argparse.ArgumentParser(description='cstar_perf job client',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s', '--server', default='ws://localhost:8000/api/cluster_comms',
                        help='Server endpoint', dest='server')
    parser.add_argument('--get-credentials', dest='gen_credentials',
                        action='store_true', help='Get and/or create ECDSA key for signing requests.')

    args = parser.parse_args()

    if args.gen_credentials:
        create_credentials()
        return

    if not os.path.exists(CLIENT_CONFIG_PATH):
        print("Config file not found. run with --get-credentials to create them first.")
        exit(1)

    job_runner = JobRunner(args.server)
    job_runner.run()

if __name__ == "__main__":
    main()
