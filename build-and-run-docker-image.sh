#!/usr/bin/env bash
docker build -t build/dump-eth -f ./dumps/ethereum/Dockerfile ./
docker run -e CHAIN=ETHEREUM --net=host build/dump-eth