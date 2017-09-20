#!/usr/bin/env bash

# Create block type for bitcoin
curl -XPUT "http://localhost:9200/bitcoin/" -d '{
  "settings": {"keyspace": "blockchain"},
  "mappings": {
    "block": {
      "properties": {
        "hash": {"type": "string", "index": "not_analyzed", "include_in_all": true},
        "height": {"type": "long", "index": "not_analyzed", "include_in_all": true},
        "time": {"type": "date", "format": "epoch_second", "include_in_all": true},
        "merkleroot": {"type": "string", "index": "no", "include_in_all": true},
        "size": {"type": "integer", "index": "no", "include_in_all": false}
      }
    },
    "transaction": {
      "properties": {
        "hex": {"type": "string", "index": "not_analyzed", "include_in_all": true},
        "locktime": {"type": "date", "format": "epoch_second", "include_in_all": true}
      }
    }
  }
}'