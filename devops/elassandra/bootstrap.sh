#!/usr/bin/env bash

curl -XPUT "http://localhost:9200/bitcoin_tx/" -d '{
   "settings" : { "keyspace" : "blockchains" } },
}'

curl -XPUT "http://localhost:9200/bitcoin_block/" -d '{
   "settings" : { "keyspace" : "blockchains" } },
}'

curl -XPUT "http://localhost:9200/ethereum_tx/" -d '{
   "settings" : { "keyspace" : "blockchains" } },
}'

curl -XPUT "http://localhost:9200/ethereum_block/" -d '{
   "settings" : { "keyspace" : "blockchains" } },
}'

curl -XPUT "http://localhost:9200/bitcoin_tx/_mapping/bitcoin_tx" -d '{
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

curl -XPUT "http://localhost:9200/bitcoin_block/_mapping/bitcoin_block" -d '{
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

curl -XPUT "http://localhost:9200/ethereum_tx/_mapping/ethereum_tx" -d '{
    "ethereum_tx" : {
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

curl -XPUT "http://localhost:9200/ethereum_block/_mapping/ethereum_block" -d '{
    "ethereum_block" : {
      "properties": {
        "hash": {"type": "string", "index": "not_analyzed", "include_in_all": true, "cql_collection" : "singleton"},
        "number": {"type": "long", "index": "no", "include_in_all": true, "cql_collection" : "singleton"},
        "size": {"type": "long", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "timestamp": {"type": "date", "index": "no", "include_in_all": false, "cql_collection" : "singleton"},
        "tx_number": {"type": "integer", "index": "no", "include_in_all": false, "cql_collection" : "singleton"}
      }
    }
}'

CREATE TABLE IF NOT EXISTS blockchains.ethereum_block (
     number varint PRIMARY KEY,
     hash text,
     parent_hash text,
     timestamp timestamp,
     sha3_uncles text,
     logs_bloom text,
     transactions_root text,
     state_root text,
     receipts_root text,
     miner text,
     difficulty varint,
     total_difficulty varint,
     extra_data text,
     size bigint,
     gas_limit bigint,
     gas_used bigint,
     transactions FROZEN<list<ethereum_block_tx>>,
     uncles FROZEN<list<text>>
);


curl -XGET "http://localhost:9200/blockchains/_search?pretty&q=000000003fd0fa5f78eea07b6daf176bfab63fb28a56768e7bbce39f047a7c14"