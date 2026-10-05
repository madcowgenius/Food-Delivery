FROM eclipse-temurin:21-jre

WORKDIR /app

COPY target/bin/payment_service.jar ./service.jar
COPY Config.toml .

EXPOSE 8084

CMD ["java", "-jar", "service.jar"]
