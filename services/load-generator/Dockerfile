# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -pl services/load-generator -am -DskipTests -B -q clean package

FROM eclipse-temurin:21-jre-jammy
RUN useradd --system --uid 10001 app
WORKDIR /app
COPY --from=build /workspace/services/load-generator/target/load-generator-*.jar /app/app.jar
COPY --from=build /workspace/services/load-generator/target/lib /app/lib
RUN chown -R app:app /app
USER 10001
ENV JAVA_OPTS="-Xmx192m -Xms64m"
# Entry point reads CLI args passed via `command:` in compose or `docker run ...`.
ENTRYPOINT ["sh","-c","exec java $JAVA_OPTS -jar /app/app.jar \"$@\"","--"]
