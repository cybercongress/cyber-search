#!/usr/bin/env bash
docker build -t build/pump-eth -f ./pumps/ethereum/Dockerfile ./
docker run -e CHAIN=ETHEREUM -e KAFKA_TRANSACTION_BATCH=1000 --net=host build/pump-eth