# Build Stage
FROM openjdk:9-jdk-slim AS build
COPY ./ /build
WORKDIR /build
RUN ./gradlew clean :pumps:ethereum:assemble

# Container with application
FROM openjdk:9-jre-slim
COPY --from=build /build/pumps/ethereum/build/libs /cyberapp/bin
ENTRYPOINT exec java $JAVA_OPTS -jar /cyberapp/bin/ethereum.jar