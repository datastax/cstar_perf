Architecture
============

cstar_perf is both a featureful web-based testing service, as well as a collection of command line and software libraries that can individually be used to incorporate distributed cassandra testing in other projects.

Here's a list of the major pieces that make up cstar_perf:

fab_cassandra.py
-----

This is a [fabric](http://www.fabfile.org) script that handles the
details of setting up a Cassandra cluster on a cluster of machines. It
can check out Cassandra source from git, build it, configure it, start
it, run testing workloads, and monitor the cluster.

## benchmark.py

A higher abstraction above fab_cassandra.py that handles common
cluster and benchmarking tasks. 

## bootstrap.py

Command line tool to configure and start a Cassandra cluster. Uses the benchmark.py API.

## stress_compare.py

Command line tool to setup one or more test scenarios and compare performance. Uses the benchmark.py API.

## cstar_perf_frontend

cstar_perf_frontend uses the above tools to setup a web frontend to
easily create test jobs and archive the results.

 * cstar_perf_server

   A web server that provides the user interface and backend API for
talking to a test clusters. 

 * cstar_perf_client

   The daemon to run on the test cluster that accepts jobs from
cstar_perf_server. Wraps stress_compare.py.
