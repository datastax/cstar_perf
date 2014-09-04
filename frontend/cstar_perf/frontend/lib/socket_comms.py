import json
import uuid
import logging
import Queue
import threading
import base64

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('cstar_perf.socket_comms')
from flask.json import JSONEncoder
from websocket import WebSocket

from cstar_perf.frontend import KEEPALIVE_MARKER, EOF_MARKER

class UnauthenticatedError(Exception):
    pass

class FaultSuppressedWebSocket(WebSocket):
    """A WebSocket that suppresses send() errors when not connected

    This does not suppress recv() errors. You need to catch socket.error exceptions yourself.
    """
    def send(self, payload, opcode=1):
        if self.connected:
            try:
                WebSocket.send(self, payload, opcode)
            except socket.error:
                pass

class CommandResponseBase(dict):
    """Base class for Command and Response classes"""
    def __init__(self, ws, data):
        self.update(data)
        self.ws = ws
        self._verify_type()
    
    def _verify_type(self):
        raise TypeError("Each subclass of CommandResponseBase must implement it's own _verify_type method")

    def respond(self, follow_up=True, **kwargs):
        """Send a response back to the peer
        
        Normally, a response requires another response from the peer until done=True. If follow_up==False, then we will not look for a response yet. This is used in action='wait' responses where we are going to send multiple responses in a row to the peer.
        """
        if self['type'] == 'response':
            assert self['done'] == False, "Can't respond to a response that is already marked done."
        data = {'command_id': self['command_id'], 'type':'response'}
        data.update(kwargs)
        if not data.has_key('done'):
            data['done'] = False
        data_str = json.dumps(data, cls=JSONEncoder)
        log.debug("Sending response : {data}".format(data=data_str))
        self.ws.send(data_str)
        if data['done'] == False and follow_up:
            # We are still expecting a response to our response:
            return self.receive()

    def receive(self):
        """Await for further response from our peer"""
        response = json.loads(self.ws.receive())
        log.debug("Received response : {response}".format(response=response))
        if response.get('type') == 'response':
            if response.get('command_id') == self['command_id']:
                return Response(self.ws, response)
            else:
                raise AssertionError("Unexpected response id in : {stuff}".format(stuff=response))
        else:
            raise AssertionError("Was expecting a response, instead got {stuff}".format(stuff=response))

    def recv(self):
        """Alias for receieve() compatible with WebSocket interface"""
        return self.receive()
        

class Command(CommandResponseBase):
    """A command our other peer asked us to do, or we want them to do"""
    def _verify_type(self):
        assert self.get('type') == 'command' and self.has_key('command_id') and self.has_key('action')

    @staticmethod
    def new(ws, **kwargs):
        """Create a new Command to be sent to our peer.
        This differs from the __init__ - that is reserved for instantiating commands originating FROM our peer."""
        assert 'command_id' not in kwargs, "New commands should not be given a command_id yet"
        assert 'type' not in kwargs, "New commands don't need to be passed a type parameter, one will be assigned."
        data = {'command_id': str(uuid.uuid1()),
                'type': 'command'}
        data.update(kwargs)
        cmd = Command(ws, data)
        cmd.new_command = True
        return cmd

    def send(self, await_response=True):
        """Send this command to our peer for them to execute"""
        #In order to send a command, there needs to be a
        #new_command==True flag on this command. Otherwise, this
        #command will be assumed to have originated from our peer and
        #should not be sent.
        if not getattr(self, 'new_command', False):
            raise AssertionError("Cannot send command that is not marked as new_command")
        data = json.dumps(self, cls=JSONEncoder)
        log.debug("Sending command : {data}".format(data=data))
        self.ws.send(data)
        if not await_response:
            return
        # Wait for the response:
        response = json.loads(self.ws.receive())
        log.debug("Received response : {response}".format(response=response))
        if response.get('type') == 'response':
            if response.get('command_id') == self['command_id']:
                return Response(self.ws, response)
            else:
                raise AssertionError("Unexpected response id in : {stuff}".format(stuff=response))
        else:
            raise AssertionError("Was expecting a response, instead got {stuff}".format(stuff=response))



class Response(CommandResponseBase):
    """A response to a Command, or a follow up response to a Response"""
    def _verify_type(self):
        assert self.get('type') == 'response' and self.has_key('command_id')

def receive_data(ws):
    data = json.loads(ws.receive())
    if data.get('type') == 'command':
        log.debug("Received command : {response}".format(response=data))
        return Command(ws, data)
    elif data.get('type') == 'response':
        log.debug("Received response : {response}".format(response=data))
        return Response(ws, data)
    else:
        raise ValueError('Unknown response type: {rtype}'.format(rtype=data.get('type')))

def receive_stream(ws, command, frame_callback):
    """Receive streaming binary data
    
    ws - the open websocket
    command - the Command object that issued the stream action. This will include the following things:
       - keepalive - denotes a frame that should be ignored, just for keeping the connection alive.
       - eof - denotes a frame that marks the end of the stream. This method will return True when it encounters this.
    frame_callback is a function to call on each non keepalive, non eof, frame. It takes a single argument for the frame data.
    """
    binary = command.get('binary', False)
    while True:
        data = ws.receive()
        if binary:
            data = base64.b64decode(data)
        if data == command['eof']:
            break
        elif data == command['keepalive']:
            pass
        else:
            frame_callback(data)
