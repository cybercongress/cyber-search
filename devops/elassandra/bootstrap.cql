CREATE KEYSPACE blockchains
  WITH REPLICATION = {
   'class' : 'NetworkTopologyStrategy',
   'LOCAL_DEVELOPMENT' : 1
  };