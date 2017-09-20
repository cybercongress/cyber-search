#!/usr/bin/env bash

# Create block type for bitcoin
curl -XPUT "http://localhost:9200/bitcoin/" -d '{
  "settings": {"keyspace": "blockchain"},
  "mappings": {
    "bitcoin": {
      "properties": {
        "hash": {"type": "string", "index": "not_analyzed", "include_in_all": true},
        "height": {"type": "long", "index": "not_analyzed", "include_in_all": true},
        "time": {"type": "date", "format": "epoch_millis", "include_in_all": true},
        "merkleroot": {"type": "string", "index": "no", "include_in_all": true},
        "size": {"type": "integer", "index": "no", "include_in_all": false}
      }
    },
    "bitcoin_tx": {
      "properties": {
        "hash": {"type": "string", "index": "not_analyzed", "include_in_all": true},
        "lock_time": {"type": "date", "format": "epoch_millis", "index": "no", "include_in_all": false},
        "block_number": {"type": "long", "index": "no", "include_in_all": false},
        "fee": {"type": "double", "index": "no", "include_in_all": false}
      }
    }
  }
}'