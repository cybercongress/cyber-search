#!/usr/bin/env bash
docker build -t build/pump-eth -f ./pumps/ethereum/Dockerfile ./
docker run -e CHAIN_FAMILY=ETHEREUM -e CS_LOG_LEVEL=TRACE --net=host build/pump-eth