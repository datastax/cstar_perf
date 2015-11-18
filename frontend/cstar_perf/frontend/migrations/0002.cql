/*

* Ensure you have an up-to-date snapshot of the test_artifacts data.

* Create a cql backup file with cassandradump that will be restored in the new table. (This need 22G of free disk space on cstar)

  python cassandradump.py --export-file /mnt/data/dump.cql --cf cstar_perf.test_artifacts --no-create

  -> https://github.com/gianlucaborello/cassandradump

* Fix the column description to  name in that file using sed. (This needs ~20G free disk space on cstar)

  sed -i.bak s/\"description\"/\"name\"/g /mnt/data/dump.cql && rm /mnt/data/dump.cql.bak

* Execute this CQL script

* Restore the data using cqlsh. I suggest to split the big dump.cql file if it's too big.

  cd /mnt/data
  split -l 100 dump.cql
  for i in `ls x*`; do echo $i; cqlsh < $i; done
  rm /mnt/data/x*

*/

DROP TABLE cstar_perf.test_artifacts;

CREATE TABLE cstar_perf.test_artifacts (
    test_id timeuuid,
    artifact_type text,
    artifact blob,
    name text,
    PRIMARY KEY (test_id, artifact_type, name)
) WITH CLUSTERING ORDER BY (artifact_type ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
