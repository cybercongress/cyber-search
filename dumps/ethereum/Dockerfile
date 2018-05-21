# Build Stage
FROM openjdk:10-jdk-slim AS build
COPY ./ /build
WORKDIR /build
RUN ./gradlew clean :dumps:ethereum:assemble

# Container with application
FROM openjdk:10-jre-slim
COPY --from=build /build/dumps/ethereum/build/libs /cyberapp/bin
ENV JAVA_CGROUP_OPTS = "-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=1 -XshowSettings:vm"
ENTRYPOINT exec java $JAVA_CGROUP_OPTS $JAVA_OPTS -jar /cyberapp/bin/ethereum.jar