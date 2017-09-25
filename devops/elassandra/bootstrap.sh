#!/usr/bin/env bash

curl -XPUT "http://localhost:9200/blockchains/" -d '{
   "settings" : { "keyspace" : "blockchains" } },
}'

curl -XPUT "http://localhost:9200/blockchains/_mapping/bitcoin_tx" -d '{
    "bitcoin_tx" : {
      "properties": {
        "txid": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "block_number": {"type": "long", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "lock_time": {"type": "long", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "fee": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'