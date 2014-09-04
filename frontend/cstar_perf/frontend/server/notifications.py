"""A collection of ZeroMQ servers

test_notification_service - send out notifications of test status changes
 - Registers a PULL socket that model.py sends notifications of tests to.
 - Registers a PUB socket that broadcasts notifications to cluster_api websocket subscribers.

console_monitor_service - monitor the console out of a cluster

"""

import zmq
from daemonize import Daemonize
import argparse
import logging
import traceback
import threading
import time
from collections import defaultdict, deque
from functools import partial
import json
import datetime

log = logging.getLogger(__name__)


TEST_NOTIFICATION_PORT_PUSH = 5556
TEST_NOTIFICATION_PORT_SUB = 5557
CONSOLE_MONITOR_PORT_PUSH = 5558
CONSOLE_MONITOR_PORT_SUB = 5559

def test_notification_service(port_pull=TEST_NOTIFICATION_PORT_PUSH, port_pub=TEST_NOTIFICATION_PORT_SUB, ip='127.0.0.1'):
    url_pull = "tcp://{ip}:{port_pull}".format(**locals())
    url_pub = "tcp://{ip}:{port_pub}".format(**locals())
    try:
        log.info('test_notification_service staring')
        log.info('test_notification_service pull url: {url_pull}'.format(**locals()))
        log.info('test_notification_service pub url: {url_pub}'.format(**locals()))
        context = zmq.Context()

        receiver = context.socket(zmq.PULL)
        receiver.bind(url_pull)

        publisher = context.socket(zmq.PUB)
        publisher.bind(url_pub)

        while True:
            data = receiver.recv_string()
            log.info('notification: {data}'.format(data=data))
            publisher.send_string(data)

    except Exception, e:
        # Log every error. If we're not running in the foreground, we
        # won't see the errrors any other way:
        log.error(traceback.format_exc())
        log.info("test_notification_service shutdown")

def console_monitor_service(port_pull=CONSOLE_MONITOR_PORT_PUSH, port_pub=CONSOLE_MONITOR_PORT_SUB, ip='127.0.0.1'):
    url_pull = "tcp://{ip}:{port_pull}".format(**locals())
    url_pub = "tcp://{ip}:{port_pub}".format(**locals())
    try:
        log.info('console_monitor_service staring')
        log.info('console_monitor_service pull url: {url_pull}'.format(**locals()))
        log.info('console_monitor_service pub url: {url_pub}'.format(**locals()))
        context = zmq.Context()

        receiver = context.socket(zmq.PULL)
        receiver.bind(url_pull)

        publisher = context.socket(zmq.XPUB)
        publisher.bind(url_pub)

        poller = zmq.Poller()
        poller.register(receiver, zmq.POLLIN)
        poller.register(publisher, zmq.POLLIN)

        # Cache the last 100 messages per cluster:
        cache = defaultdict(partial(deque, maxlen=100)) # cluster_name -> deque

        while True:
            events = dict(poller.poll(1000))
            
            if receiver in events:
                data = receiver.recv()
                topic, cluster, msg = data.split(' ', 2)
                cache[cluster].append(msg)
                # Mark message as realtime:
                msg = json.loads(msg)
                msg['realtime'] = True
                msg = json.dumps(msg)
                data = " ".join([topic, cluster, msg])
                log.debug("PUB - {msg}".format(msg=data))
                publisher.send(data)
            
            if publisher in events:
                event = publisher.recv()
                # Subscription events areone byte: 0=unsub or 1=sub,
                # followed by topic:
                if event[0] == b'\x01':
                    topic, cluster = event[1:].strip().split(" ")
                    log.debug("SUBSCRIBE - {sub}".format(sub=event[1:]))
                    if topic == 'console':
                        # Client subscribed, send out previous messages:
                        log.debug("Sending backlog:")
                        for msg in cache[cluster]:
                            # Mark messages as non-realtime:
                            data = json.loads(msg)
                            data['realtime'] = False
                            msg = json.dumps(data)
                            data = "console {cluster} {msg}".format(cluster=cluster, msg=msg)
                            log.debug(data)
                            publisher.send(data)
                elif event[0] == b'\x00':
                    log.debug("UNSUBSCRIBE - {sub}".format(sub=event[1:]))
                    

    except Exception, e:
        # Log every error. If we're not running in the foreground, we
        # won't see the errrors any other way:
        log.error(traceback.format_exc())
        log.info("console_monitor_service shutdown")

def multi_service():
    """Start all the services in separate threads"""
    threads = []
    for service in [test_notification_service, console_monitor_service]:
        threads.append(threading.Thread(target=service))
    for thread in threads:
        thread.daemon = True
        thread.start()
    while threading.active_count() > 0:
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            exit()

def zmq_socket_subscribe(url, topic='', timeout=5000):
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.SUB)
    zmq_socket.connect(url)
    zmq_socket.setsockopt_string(
        zmq.SUBSCRIBE, 
        unicode(topic))
    # Timeout:
    zmq_socket.setsockopt(zmq.RCVTIMEO, timeout)
    return zmq_socket
    
def console_publish(cluster_name, data):
    """Publish a console message or control message

    cluster_name - the name of the cluster the data came from
    data - a dictionary containing the following:
       job_id - the job id the cluster is currently working on
       msg - a message shown on the console
       ctl - A control message indicating cluster status START, DONE, IDLE
    """
    zmq_context = zmq.Context()
    zmq_socket = zmq_context.socket(zmq.PUSH)
    zmq_socket.connect("tcp://127.0.0.1:{port}".format(port=CONSOLE_MONITOR_PORT_PUSH))
    if not data.has_key('timestamp'):
        data['timestamp'] = (datetime.datetime.utcnow() - datetime.datetime(1970,1,1)).total_seconds()
    zmq_socket.send_string("console {cluster_name} {data}".format(
        cluster_name=cluster_name, 
        data=json.dumps(data)))

def console_subscribe(cluster_name):
    return zmq_socket_subscribe(
        'tcp://localhost:{port}'.format(port=CONSOLE_MONITOR_PORT_SUB), 
        'console {cluster_name} '.format(cluster_name=cluster_name))



def main():
    parser = argparse.ArgumentParser(description='cstar_perf_notifications')
    parser.add_argument('-F', '--foreground', dest='foreground', 
                        action='store_true', help='Run in the foreground instead of daemonizing')
    parser.add_argument('--pid', default="/tmp/cstar_perf_notifications.pid", 
                        help='PID file for daemon', dest='pid')
    parser.add_argument('-l', '--log', default='/tmp/cstar_perf_notifications.log',
                        help='File to log to', dest='logfile')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print log messages', dest='verbose')
    args = parser.parse_args()

    log.setLevel(logging.DEBUG)
    log.propagate = False
    fh = logging.FileHandler(args.logfile, "a")
    formatter = logging.Formatter("%(levelname)s:%(funcName)s:%(asctime) -8s %(message)s")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    log.addHandler(fh)
    if args.verbose:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        sh.setLevel(logging.DEBUG)
        log.addHandler(sh)
    keep_fds = [fh.stream.fileno()]


    if args.foreground:
        multi_service()
    else:
        daemon = Daemonize(app="notifications", pid=args.pid, action=multi_service, keep_fds=keep_fds)
        daemon.start()


if __name__ == "__main__":
    main()
