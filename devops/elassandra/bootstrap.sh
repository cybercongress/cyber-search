#!/usr/bin/env bash

curl -XPUT "http://localhost:9200/blockchains/" -d '{
   "settings" : { "keyspace" : "blockchains" } },
}'

curl -XPUT "http://localhost:9200/blockchains/_mapping/bitcoin_tx" -d '{
    "bitcoin_tx" : {
      "properties": {
        "txid": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "block_hash": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "block_number": {"type": "long", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "block_time": {"type": "date", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "fee": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "total_output": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'


curl -XPUT "http://localhost:9200/blockchains/_mapping/bitcoin_block" -d '{
    "bitcoin_block" : {
      "properties": {
        "hash": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "height": {"type": "long", "index": "no", "include_in_all": true, "cql_collection" : "singleton"},
        "time": {"type": "date", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "tx_number": {"type": "integer", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "total_outputs_value": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'

curl -XGET "http://localhost:9200/blockchains/_search?pretty&q=000000003fd0fa5f78eea07b6daf176bfab63fb28a56768e7bbce39f047a7c14"