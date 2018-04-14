#!/usr/bin/env bash
docker build -t build/dump-eth -f ./pumps/bitcoin/Dockerfile ./
docker run -e CHAIN=BITCOIN -e CS_LOG_LEVEL=TRACE --net=host build/pump-btc