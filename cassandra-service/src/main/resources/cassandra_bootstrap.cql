CREATE KEYSPACE IF NOT EXISTS search
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','DEVELOPMENT' : 1};



CREATE KEYSPACE IF NOT EXISTS bitcoin
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','DEVELOPMENT' : 1};

CREATE KEYSPACE IF NOT EXISTS bitcoin_cash
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','DEVELOPMENT' : 1};

CREATE KEYSPACE IF NOT EXISTS ethereum
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','DEVELOPMENT' : 1};

CREATE KEYSPACE IF NOT EXISTS ethereum_classic
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','DEVELOPMENT' : 1};



CREATE KEYSPACE IF NOT EXISTS cyber_system
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','DEVELOPMENT' : 1};

CREATE KEYSPACE IF NOT EXISTS pump
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','DEVELOPMENT' : 1};



CREATE TABLE IF NOT EXISTS cyber_system.schema_version (
application_id text,
version int,
apply_time timestamp,
migration_hash int,
PRIMARY KEY (application_id, version)
) WITH CLUSTERING ORDER BY (version DESC);




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