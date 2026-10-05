FROM eclipse-temurin:21-jre

WORKDIR /app

COPY target/bin/restaurant_service.jar ./service.jar
COPY Config.toml .

EXPOSE 8082

CMD ["java", "-jar", "service.jar"]
