CREATE KEYSPACE IF NOT EXISTS blockchains
    WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','LOCAL_DEVELOPMENT' : 1};

CREATE KEYSPACE IF NOT EXISTS search
    WITH REPLICATION = {'class' : 'NetworkTopologyStrategy','LOCAL_DEVELOPMENT' : 1};