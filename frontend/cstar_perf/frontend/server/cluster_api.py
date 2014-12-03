"""Backend API to communicate with test clusters"""

import json
import base64
import uuid
import os
from flask import Flask
import zmq
import hashlib
import cStringIO

from app import app, db, sockets
from cstar_perf.frontend.lib.crypto import APIKey, BadConfigFileException
from cstar_perf.frontend.lib.util import random_token
import cstar_perf.frontend.lib.socket_comms as socket_comms
from cstar_perf.frontend.lib.socket_comms import Command, Response, receive_data, UnauthenticatedError
from cstar_perf.frontend import SERVER_KEY_PATH
from notifications import console_publish

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('cstar_perf.cluster_api')

class BadResponseError(Exception):
    pass

class CloseConnectionException(Exception):
    pass


@sockets.route('/api/cluster_comms')
def cluster_comms(ws):
    """Websocket to communicate with the test clusters

    Commands are logical actions one end of the socket wishes the
    other end to take. Responses are follow ups to a command, which
    there can be multiple, back and forth between client and server
    until the receiving end marks a response as Done.

    Command structure:
     {type:'command',
      command_id:'some unique string for this command',
      message:'some message to the other end',
      action:'some action for the receiver to take',
      // Extra parameters:
      foo: ...,
      bar: ...,
     }

    Response structure:
     {type:'response',
      command_id:'the command id this is a response to',
      message:'some message to the other end',
      done: true/false (the responder considers the command complete)
      // Extra parameters:
      foo: ...,
      bar: ...,
     }

    Possible commands:
      * authenticate - server asks client to authenticate
      * get_work - client asks for a test
      * test_done - client is done with a test, and sending artifacts
      * cancel_test - server asks client to cancel test
      * shutdown - server asks client to shutdown service

    Protocol:
     Authentication:
      * client initiates connection to this server
      * server sends client a random challenge token
        {type:'command', command_id='zzzz', action:'authenticate', token:'xxxxxxx'}
      * client signs challenge token with it's private key ands sends the signature
        {type:'response', command_id='zzz', cluster:'bdplab', signature:'xxxxxxxx'}
      * server verifies the signature is against the token it sent and the public
        key it has on file for the cluster.
      * server sends a 'you are authenticated' response.
        {type:'response', command_id='zzz', authenticated: true, done:true}

     Task loop:
      * client sends a 'give me work' request.
        {type:'command', command_id='yyy', action:'get_work'}
      * server sends a 'ok, wait for work' response.
        {type:'response', command_id='yyy', action:'wait'}
      * server sends a single test to the cluster
        {type:'response', command_id='yyy', test:{...}}
      * client responds 'ok, received test' response
        {type:'response', command_id:'yyy', test_id:'xxxxxxx'}
      * server updates status of test to in_progress in database
        {type:'response', command_id:'yyy', message:'test_updated', done:true}
      * client sends artifacts via streaming protocol (See below)
      * client sends 'ok, test done, artifacts sent.' request.
        {type:'command', command_id:'llll', action:'test_done', test_id:'xxxxxxx'}
      * server updates status of test to completed
      * server sends a 'ok, test updated' response
        {type:'response', command_id:'llll', test_id:'xxxxxx', message='test_update', done:true}

     Streaming:
      protocol for streaming raw data: console output, binary artifacts etc.
      * Sending peer sends a "I'm going to send binary data to you" request:
        {type:'command', command_id='xxx', action:'stream', test_id='xxxxx', 
         kind:"[console|failure|chart|system_logs|stress_logs]", name='name', 
         eof='$$$EOF$$$', keepalive='$$$KEEPALIVE$$$'}
      * Receiving peer sends response indicating it's ready to receive the stream:
        {type:'response', command_id='xxx', action='ready'}
      * Peer starts sending arbitrary binary data messages.
      * The receiving peer reads binary data. If it encounters $$$KEEPALIVE$$$ as it's own message, it will 
        omit that data, as it's only meant to keep the socket open.
      * Once $$$EOF$$$ is seen by the receiving peer, in it's own message, the receiving peer can respond:
        {type:'response', command_id='xxx', message:'stream_received', done:true}

    """
    context = {'apikey': APIKey.load(SERVER_KEY_PATH),
               'cluster': None}

    def authenticate():
        token_to_sign = random_token()
        cmd = Command.new(ws, action='authenticate', token=token_to_sign)
        response = cmd.send()
        context['cluster'] = cluster = response['cluster']
        client_pubkey = db.get_pub_key(cluster)
        client_apikey = APIKey(client_pubkey['pubkey'])
        
        # Verify the client correctly signed the token:
        try:
            client_apikey.verify_message(token_to_sign, response.get('signature'))
        except:
            response.respond(message='Bad Signature of token for authentication', done=True)
            log.error('client provided bad signature for auth token')
            raise

        response.respond(authenticated=True, done=True)

        # Client will ask us to authenticate too:
        command = receive_data(ws)
        assert command.get('action') == 'authenticate'
        data = {'signature' :context['apikey'].sign_message(command['token'])}
        response = command.respond(**data)
        if response.get('authenticated') != True:
            raise UnauthenticatedError("Our peer could not validate our signed auth token")

    def get_work(command):
        # Mark any existing in_process jobs for this cluster as
        # failed. If the cluster is asking for new work, then these
        # got dropped on the floor:
        for test in db.get_in_progress_tests(context['cluster']):
            db.update_test_status(test['test_id'], 'failed')

        # Find the next test scheduled for the client's cluster:
        tests = db.get_scheduled_tests(context['cluster'], limit=1)
        if len(tests) > 0:
            test_id = tests[0]['test_id']
        else:
            # No tests are currently scheduled.
            # Register a zmq listener of notifications of incoming tests, with a timeout.
            # When we see any test scheduled notification for our cluster, redo the query.
            # If timeout reached, redo the query anyway in case we missed the notification.
            def setup_zmq():
                zmq_context = zmq.Context()
                zmq_socket = zmq_context.socket(zmq.SUB)
                zmq_socket.connect('tcp://127.0.0.1:5557')
                zmq_socket.setsockopt_string(
                    zmq.SUBSCRIBE, 
                    unicode('scheduled {cluster} '.format(cluster=context['cluster'])))
                zmq_socket.setsockopt(zmq.RCVTIMEO, 15000)
                return zmq_socket
            zmq_socket = setup_zmq()
            while True:
                try:
                    cluster, test_id = zmq_socket.recv_string().split()
                except zmq.error.Again:
                    pass
                except zmq.error.ZMQError, e:
                    if e.errno == zmq.POLLERR:
                        log.error(e)
                        # Interrupted zmq socket code, reinitialize:
                        # I get this when I resize my terminal.. WTF?
                        zmq_socket = setup_zmq()
                finally:
                    tests = db.get_scheduled_tests(context['cluster'], limit=1)
                    if len(tests) > 0:
                        test_id = tests[0]['test_id']
                        break
                    else:
                        # Send no-work-yet message:
                        console_publish(context['cluster'], {'ctl':'WAIT'})
                        command.respond(action='wait', follow_up=False)
        test = db.get_test(test_id)
        # Give the test to the client:
        response = command.respond(test=test)
        # Expect an prepared status message back:
        assert response['test_id'] == test['test_id'] and \
            response['status'] == 'prepared'
        # Update the test status:
        db.update_test_status(test['test_id'], 'in_progress')
        # Let the client know they can start it:
        response.respond(test_id=test['test_id'], status="in_progress", done=True)

    def test_done(command):
        """Receive completed test artifacts from client"""
        db.update_test_status(command['test_id'], command['status'])
        # Record test failure message, if any:
        if command['status'] == 'failed':
            msg = (command.get('message','') + "\n" + command.get('stacktrace','')).strip()
            db.update_test_artifact(command['test_id'], 'failure', msg)
        # Send response:
        command.respond(test_id=command['test_id'], message='test_update', done=True)

    def receive_stream(command):
        """Receive a stream of data"""
        command.respond(message="ready", follow_up=False)
        log.debug("Receving data stream ....")
        if command['kind'] == 'console':
            console_dir = os.path.join(os.path.expanduser("~"), ".cstar_perf", "console_out")
            try:
                os.makedirs(console_dir)
            except OSError:
                pass
            console = open(os.path.join(console_dir, command['test_id']), "w")
        tmp = cStringIO.StringIO()
        sha = hashlib.sha256()
        try:
            def frame_callback(frame, binary):
                if not binary:
                    frame = frame.encode("utf-8")
                if command['kind'] == 'console':
                    console.write(frame)
                    console_publish(context['cluster'], {'job_id':command['test_id'], 'msg':frame})
                    console.flush()
                else:
                    console_publish(context['cluster'], {'job_id':command['test_id'], 'ctl':'IN_PROGRESS'})
                sha.update(frame)
                tmp.write(frame)
            socket_comms.receive_stream(ws, command, frame_callback)
            if command['kind'] == 'console':
                console.close()
            # TODO: confirm with the client that the sha is correct
            # before storing
        finally:
            # In the event of a socket error, we always want to commit
            # what we have of the artifact to the database. Better to
            # have something than nothing. It's the client's
            # responsibility to resend artifacts that failed.

            db.update_test_artifact(command['test_id'], command['kind'], tmp, command['name'])

        command.respond(message='stream_received', done=True, sha256=sha.hexdigest())
        
    # Client and Server both authenticate to eachother:
    authenticate()
    
    try:
        # Dispatch on client commands:
        while True:
            command = receive_data(ws)
            assert command['type'] == 'command'
            if command['action'] == 'get_work':
                console_publish(context['cluster'], {'ctl':'WAIT'})
                get_work(command)
            elif command['action'] == 'test_done':
                console_publish(context['cluster'], {'ctl':'DONE'})
                test_done(command)
            elif command['action'] == 'stream':
                receive_stream(command)
            elif command['action'] == 'good_bye':
                log.info("client said good_bye. Closing socket.")
                break
    finally:
        console_publish(context['cluster'], {'ctl':'GOODBYE'})
