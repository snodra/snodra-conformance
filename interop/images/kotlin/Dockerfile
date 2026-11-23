# syntax=docker/dockerfile:1
# The Kotlin conformance service of snodra-messaging (examples/conformance-service), as its Spring
# Boot jar. The source arrives as the named build context `kotlinsrc` (compose.yaml): a pinned
# tag of https://github.com/snodra/snodra-messaging, or a local checkout (KOTLIN_SRC).
FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
COPY --from=kotlinsrc . .
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew :examples:conformance-service:bootJar --no-daemon -x test \
    && cp examples/conformance-service/build/libs/*.jar /app.jar

FROM eclipse-temurin:25-jre
COPY --from=build /app.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
