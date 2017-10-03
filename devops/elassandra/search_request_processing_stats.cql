CREATE TYPE IF NOT EXISTS search.document_key (
    id text,
    type text,
    catalog text
);


CREATE TABLE IF NOT EXISTS search.request_processing_stats (
     raw_request text,
     time timestamp,
     time_m int,
     max_score float,
     total_hits bigint,
     search_time_ms bigint,
     documents frozen<list<search.document_key>>,
     PRIMARY KEY (time_m,time,raw_request)
);