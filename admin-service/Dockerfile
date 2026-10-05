FROM eclipse-temurin:21-jre

WORKDIR /app

COPY target/bin/admin_service.jar ./service.jar
COPY Config.toml .

EXPOSE 8087

CMD ["java", "-jar", "service.jar"]
