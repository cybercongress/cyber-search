docker build -t build/pump-eth -f ./pumps/ethereum/Dockerfile ./
docker run -e CHAIN=ETHEREUM -e JAVA_OPTS="-Xmx2g" --net=host build/pump-eth