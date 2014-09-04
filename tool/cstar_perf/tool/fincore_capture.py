#!/bin/env python

"""Tool to continuously capture fincore stats from Cassandra data files related to stress"""

import subprocess
import shlex
import time
import argparse
from daemonize import Daemonize

import logging

def capture_stats():
    while True:
        proc = subprocess.Popen("/usr/local/bin/linux-fincore /mnt/d*/cassandra/{data,flush}/Keyspace1/Standard1*/*", 
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        stdout,stderr = proc.communicate()
        logger.info("linux-fincore stats : \n"+stdout)
        time.sleep(int(args.interval))

if __name__ == "__main__":
    pid = "/tmp/fincore_capture.pid"
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description='fincore_capture')
    parser.add_argument('-f', '--file', default='/tmp/fincore.stats.log', 
                        help='File to log to', dest='logfile')
    parser.add_argument('-i', '--interval', dest='interval', 
                        default='10', help='Time interval, in seconds, to run fincore.')    
    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    fh = logging.FileHandler(args.logfile, "a")
    formatter = logging.Formatter("%(levelname)s:%(funcName)s:%(asctime) -8s %(message)s")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    keep_fds = [fh.stream.fileno()]

    daemon = Daemonize(app="fincore_capture", pid='/tmp/fincore_capture.pid', action=capture_stats, keep_fds=keep_fds)
    daemon.start()


