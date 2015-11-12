CREATE TABLE cstar_perf.test_artifacts_backup (
    test_id timeuuid,
    artifact_type text,
    artifact blob,
    description text,
    PRIMARY KEY (test_id, artifact_type)
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

COPY cstar_perf.test_artifacts TO '/tmp/test_artifacts';

# DONT FORGET TO SET '[csv] field_size_limit=1000000000' in ~/.cassandra/cqlshrc
COPY cstar_perf.test_artifacts_backup FROM '/tmp/test_artifacts';

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

COPY cstar_perf.test_artifacts (test_id, artifact_type, artifact, name) FROM '/tmp/test_artifacts';
