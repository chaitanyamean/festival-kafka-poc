# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -pl services/notification-service -am -DskipTests -B -q clean package

FROM eclipse-temurin:21-jre-jammy
# OTel Java agent (zero-code instrumentation). Version pinned via build-arg so a
# bump happens in one place in parent pom.xml. Agent is picked up at runtime via
# JAVA_TOOL_OPTIONS (set in docker-compose.yml), which the JVM prepends to any
# command line — so JAVA_OPTS stays free for -Xmx/-Xms tuning.
ARG OTEL_AGENT_VERSION=2.9.0
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar /opt/opentelemetry-javaagent.jar
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 app \
 && chmod 644 /opt/opentelemetry-javaagent.jar
WORKDIR /app
COPY --from=build /workspace/services/notification-service/target/notification-service-*.jar /app/app.jar
RUN chown app:app /app/app.jar
USER 10001
EXPOSE 8080
ENV JAVA_OPTS="-Xmx256m -Xms128m -XX:MaxRAMPercentage=75"
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=20 \
    CMD curl -fsS http://localhost:8080/actuator/health/readiness || exit 1
ENTRYPOINT ["sh","-c","exec java $JAVA_OPTS -jar /app/app.jar"]
