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

curl -XGET "http://localhost:9200/blockchains/_search?pretty&q=0000000018920212d4d4dcddb6e24f37d23b35a0078d270227c83051bb350049"