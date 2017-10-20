#!/usr/bin/env bash

curl -XPUT "http://localhost:9200/bitcoin_tx/" -d '{
   "settings" : { "keyspace" : "bitcoin" } },
}'

curl -XPUT "http://localhost:9200/bitcoin_block/" -d '{
   "settings" : { "keyspace" : "bitcoin" } },
}'

curl -XPUT "http://localhost:9200/bitcoin_address/" -d '{
   "settings" : { "keyspace" : "bitcoin" } },
}'

curl -XPUT "http://localhost:9200/ethereum_tx/" -d '{
   "settings" : { "keyspace" : "ethereum" } },
}'

curl -XPUT "http://localhost:9200/ethereum_block/" -d '{
   "settings" : { "keyspace" : "ethereum" } },
}'

curl -XPUT "http://localhost:9200/ethereum_address/" -d '{
   "settings" : { "keyspace" : "ethereum" } },
}'

curl -XPUT "http://localhost:9200/bitcoin_tx/_mapping/tx" -d '{
    "tx" : {
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

curl -XPUT "http://localhost:9200/bitcoin_block/_mapping/block" -d '{
    "block" : {
      "properties": {
        "hash": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "height": {"type": "long", "index": "no", "include_in_all": true, "cql_collection" : "singleton"},
        "time": {"type": "date", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "tx_number": {"type": "integer", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "total_outputs_value": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'

curl -XPUT "http://localhost:9200/bitcoin_address/_mapping/address" -d '{
    "address" : {
      "properties": {
        "id": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "tx_number": {"type": "integer", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "balance": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "total_received": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'

curl -XPUT "http://localhost:9200/ethereum_tx/_mapping/tx" -d '{
    "tx" : {
      "properties": {
        "hash": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "block_hash": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "block_number": {"type": "long", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "timestamp": {"type": "date", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "fee": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "value": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'

curl -XPUT "http://localhost:9200/ethereum_block/_mapping/block" -d '{
    "block" : {
      "properties": {
        "hash": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "number": {"type": "long", "index": "no", "include_in_all": true, "cql_collection" : "singleton"},
        "size": {"type": "long", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "timestamp": {"type": "date", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "tx_number": {"type": "integer", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'

curl -XPUT "http://localhost:9200/ethereum_address/_mapping/address" -d '{
    "address" : {
      "properties": {
        "id": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "is_contract_address": {"type": "boolean", "index": "not_analyzed", "include_in_all": false, "cql_collection" : "singleton"},
        "tx_number": {"type": "integer", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "balance": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "total_received": {"type": "string", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'