# Build Stage
FROM openjdk:10-jdk-slim AS build
COPY ./ /build
WORKDIR /build
RUN ./gradlew clean :search-api:assemble

# Container with application
FROM openjdk:10-jre-slim
COPY --from=build /build/search-api/build/libs /cyberapp/bin
ENTRYPOINT ["java", "-jar", "/cyberapp/bin/search-api.jar"]