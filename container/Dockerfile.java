FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY java/pom.xml ./pom.xml
COPY java/src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/sistema-mensagens-1.0.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
