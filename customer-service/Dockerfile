FROM eclipse-temurin:21-jre

WORKDIR /app

COPY target/bin/customer_service.jar ./service.jar
COPY Config.toml .

EXPOSE 8081

CMD ["java", "-jar", "service.jar"]
