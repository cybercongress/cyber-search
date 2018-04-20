# Build Stage
FROM openjdk:10-jdk-slim AS build
COPY ./ /build
WORKDIR /build
RUN ./gradlew clean :contract-summary:ethereum:assemble

# Container with application
FROM openjdk:10-jre-slim
COPY --from=build /build/contract-summary/ethereum/build/libs /cyberapp/bin
ENTRYPOINT exec java $JAVA_OPTS -jar /cyberapp/bin/ethereum.jar