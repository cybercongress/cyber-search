# Container with application
FROM openjdk:10-jre-slim
COPY /build/libs /cyberapp/bin
ENTRYPOINT exec java $JAVA_OPTS -jar /cyberapp/bin/ethereum.jar