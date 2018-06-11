# Container with application
FROM openjdk:8-jre-slim
COPY /build/libs /cyberapp/bin
ENTRYPOINT ["java", "-jar", "/cyberapp/bin/search-api.jar"]