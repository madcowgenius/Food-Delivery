FROM eclipse-temurin:21-jre

WORKDIR /app

COPY target/bin/delivery_service.jar ./service.jar
COPY Config.toml .

EXPOSE 8085

CMD ["java", "-jar", "service.jar"]
