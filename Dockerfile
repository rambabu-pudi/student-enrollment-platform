# syntax=docker/dockerfile:1
# Multi-stage build: the JDK and Gradle cache never reach the runtime image.
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace

COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
COPY common ./common
COPY ingestion-service ./ingestion-service
COPY processing-service ./processing-service
COPY query-service ./query-service

ARG MODULE
RUN chmod +x ./gradlew && ./gradlew :${MODULE}:bootJar --no-daemon -x test

FROM eclipse-temurin:21-jre-alpine AS runtime
ARG MODULE
WORKDIR /app

# Never run as root.
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /workspace/${MODULE}/build/libs/*.jar app.jar
RUN chown -R app:app /app
USER app

# Container-aware heap sizing plus a GC suited to high-allocation streaming workloads.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
