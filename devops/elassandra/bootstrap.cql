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