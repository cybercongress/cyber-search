CREATE TABLE IF NOT EXISTS pump.indexing_progress (
       application_id text PRIMARY KEY,
       block_number bigint,
       block_hash text,
       index_time timestamp
);