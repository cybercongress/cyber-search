# Build Stage
FROM openjdk:10-jdk-slim AS build
COPY ./ /build
WORKDIR /build
RUN ./gradlew clean :dumps:ethereum:assemble

# Container with application
FROM openjdk:10-jre-slim
COPY --from=build /build/dumps/ethereum/build/libs /cyberapp/bin
ENTRYPOINT ["java", "-jar", "/cyberapp/bin/ethereum.jar"]