# Build Stage
FROM openjdk:8u131-jdk AS build

COPY ./ /cybernode-connectors-build
WORKDIR /cybernode-connectors-build

RUN ./gradlew clean build

ARG STREAM_REACTOR_URL=https://github.com/datamountaineer/stream-reactor/releases/download/0.3.0/kafka-connect-cassandra-0.3.0-3.3.0-all.tar.gz
RUN wget "${STREAM_REACTOR_URL}" -O stream-reactor.tar.gz \
    && tar -xzf stream-reactor.tar.gz --no-same-owner --strip-components=1 -C ./cyber-connectors \
    && rm stream-reactor.tar.gz



# Container with application
FROM confluentinc/cp-kafka-connect:3.3.0
ENV CONNECT_PLUGIN_PATH=/etc/kafka-connect/custom-plugins
COPY --from=build /cybernode-connectors-build/cyber-connectors /etc/kafka-connect/custom-plugins
