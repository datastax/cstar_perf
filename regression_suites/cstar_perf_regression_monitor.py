#!/usr/bin/env python2

from __future__ import unicode_literals

import sys
import argparse
import requests
import json
from Queue import Queue
from threading import Thread

tmpstart = 1448946000
tmpstop = 1451624399


class CstarPerfClient(object):
    """cstar_perf api rest client"""

    server = "cstar.datastax.com"

    urls = {
        'get_series_list': '/api/series',
        'get_series': '/api/series/{name}/{start_timestamp}/{stop_timestamp}',
        'get_test_summary': '/tests/artifacts/{job_id}/stats_summary/stats_summary.{job_id}.json'
    }

    def __init__(self, server=None):
        if server:
            self.server = server

    def build_url(self, url, **kwargs):
        url = self.urls[url].format(**kwargs)
        return "http://{}{}".format(self.server, url)


class RegressionSeries(CstarPerfClient):
    """Represent a regression serie"""

    # Serie name
    name = None

    # wall time
    start_timestamp = None
    stop_timestamp = None

    # Job ids of the serie
    job_ids = None

    # Computed metrics
    metrics = None

    has_regression = False

    def __init__(self, name, start_timestamp, stop_timestamp, **kwargs):
        super(RegressionSeries, self).__init__(**kwargs)

        self.name = name
        self.start_timestamp = start_timestamp
        self.stop_timestamp = stop_timestamp
        self.job_ids = []

        self.metrics = {}  # metrics per operation

    def __repr__(self):
        return "<RegressionSeries({}, {} tests)>".format(
            self.name, len(self.job_ids))

    def __unicode__(self):
        return self.__str__()

    def __str__(self):
        return "{}: {} tests)".format(
            self.name, len(self.job_ids))

    def _get_series(self):
        url = self.build_url('get_series', name=self.name,
            start_timestamp=self.start_timestamp, stop_timestamp=self.stop_timestamp)
        r = requests.get(url)
        if r.status_code != 200:
            raise Exception('Unable to fetch series')

        return json.loads(r.text)['series']

    def _fetch_job_stats(self, job_id):
        url = self.build_url('get_test_summary', name=self.name, job_id=job_id)
        # the summary file is small, so we currently keep it in memory
        r = requests.get(url)
        data = json.loads(r.text)

        stats = {}
        for operation in data['stats']:
            operation_name = operation.get('test', operation['id'])
            stats[operation_name] = {}
            s = stats[operation_name]
            s['op_rate'] = float(operation['op rate'].split(' ')[0])
            s['95th_percentile'] = float(operation['latency 95th percentile'].split(' ')[0])
            s['999th_percentile'] = float(operation['latency 99.9th percentile'].split(' ')[0])
            s['99th_percentile'] = float(operation['latency 99th percentile'].split(' ')[0])
            s['latency_max'] = float(operation['latency max'].split(' ')[0])
            s['latency_mean'] = float(operation['latency mean'].split(' ')[0])
            s['latency_median'] = float(operation['latency median'].split(' ')[0])

        return stats

    def _fetch_series_stats(self):
        for job_id in self.job_ids:
            stats = self._fetch_job_stats(job_id)
            for operation_name, metrics in stats.iteritems():
                if operation_name not in self.metrics:
                    self.metrics[operation_name] = {}
                    self.metrics[operation_name]['op_rate'] = []
                    self.metrics[operation_name]['95th_percentile'] = []
                    self.metrics[operation_name]['999th_percentile'] = []
                    self.metrics[operation_name]['99th_percentile'] = []
                    self.metrics[operation_name]['latency_max'] = []
                    self.metrics[operation_name]['latency_mean'] = []
                    self.metrics[operation_name]['latency_median'] = []

                self.metrics[operation_name]['op_rate'].append(metrics['op_rate'])
                self.metrics[operation_name]['95th_percentile'].append(metrics['95th_percentile'])
                self.metrics[operation_name]['999th_percentile'].append(metrics['999th_percentile'])
                self.metrics[operation_name]['99th_percentile'].append(metrics['99th_percentile'])
                self.metrics[operation_name]['latency_max'].append(metrics['latency_max'])
                self.metrics[operation_name]['latency_mean'].append(metrics['latency_mean'])
                self.metrics[operation_name]['latency_median'].append(metrics['latency_median'])

    def do_regression_check(self):
        self.job_ids = self._get_series()

        latest_job = self.job_ids.pop()
        num_jobs = len(self.job_ids)

        self._fetch_series_stats()

        # check all operations for regression
        stats = self._fetch_job_stats(latest_job)

        for operation_name, metrics in stats.iteritems():
            op_rate = metrics['op_rate']
            average_rate = sum(self.metrics[operation_name]['op_rate'])/num_jobs

            # 15% of tolerance... will be configurable
            if abs(op_rate - average_rate) > (average_rate * 0.15):
                self.has_regression = True


class RegressionMonitor(CstarPerfClient):
    """cstar_perf regression monitor tool"""

    concurrency = 1

    def _get_series(self):
        url = self.build_url('get_series_list')
        r = requests.get(url)
        if r.status_code != 200:
            raise Exception('Unable to fetch series list')

        series = json.loads(r.text)
        return [RegressionSeries(s, tmpstart, tmpstop, server=self.server) for s in series]

    def run(self):
        series = self._get_series()

        q = Queue()
        for s in series:
            q.put(s)

        def worker():
            while True:
                serie = q.get()
                serie.do_regression_check()
                q.task_done()

        for i in range(self.concurrency):
            t = Thread(target=worker)
            t.daemon = True
            t.start()

        q.join()

        for serie in series:
            if serie.has_regression:
                print 'we have a problem'

def main(args):
    monitor = RegressionMonitor(
        server=args.server,
        start_timestamp=start_timestamp,
        stop_timestamp=stop_timestamp
    )

    if args.command == 'run':
        monitor.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='cstar_cstar_perf_regression_monitor.py - '
                                     'Monitor performance regression',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser_subparsers = parser.add_subparsers(dest='command')

    run = parser_subparsers.add_parser('run', description="Run the regression monitoring process")
    run.add_argument('-s', '--server', required=False, help='The hostname of the server')
    run.add_argument('--start-timestamp', help='The start timestamp')
    run.add_argument('--stop-timestamp', help='The stop timestamp')

    try:
        args = parser.parse_args()
    finally:
        # Print verbose help if they didn't give any command:
        if len(sys.argv) == 1:
            parser.print_help()

    main(args)
