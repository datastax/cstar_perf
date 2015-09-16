.. cstar_perf documentation master file, created by
   sphinx-quickstart on Thu Sep  4 22:08:22 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

cstar_perf
==========

cstar_perf is a performance testing platform for Apache Cassandra. 

In a mostly automated fashion, it can do the following:

* Download and build Cassandra source code
* Download and setup DSE binaries
* Configure and bootstrap nodes
* Run stress workloads
* Capture performance metrics
* Create reports and charts comparing different configs/workloads
* Webserver frontend for scheduling tests, viewing prior runs, and
  monitoring test clusters.


Contents
--------

.. toctree::
  :maxdepth: 2

  setup_dev_environment
  setup_cstar_perf_tool
  setup_cstar_perf_frontend
  running_tests

  architecture
