ALTER TABLE cstar_perf.test_artifacts ADD artifact_available boolean;
ALTER TABLE cstar_perf.test_artifacts ADD object_id text;

CREATE TABLE cstar_perf.chunk_object_storage (
    object_id text,
    chunk_id int,
    chunk_size int,
    chunk_sha text,
    object_chunk blob,
    total_chunks int static,
    object_size int static,
    object_sha text static,
    primary key ((object_id), chunk_id));