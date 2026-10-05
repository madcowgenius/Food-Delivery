FROM eclipse-temurin:21-jre

WORKDIR /app

COPY target/bin/notification_service.jar ./service.jar
COPY Config.toml .

EXPOSE 8086

CMD ["java", "-jar", "service.jar"]
