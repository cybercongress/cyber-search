# Build Stage
FROM openjdk:9-jdk-slim AS build
COPY ./ /build
WORKDIR /build
RUN ./gradlew clean :pumps:bitcoin:assemble

# Container with application
FROM openjdk:9-jre-slim
COPY --from=build /build/pumps/bitcoin/build/libs /cyberapp/bin
ENTRYPOINT exec java $JAVA_OPTS -jar /cyberapp/bin/bitcoin.jar