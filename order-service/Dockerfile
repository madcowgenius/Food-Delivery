FROM eclipse-temurin:21-jre

WORKDIR /app

COPY target/bin/order_service.jar ./service.jar
COPY Config.toml .

EXPOSE 8083

CMD ["java", "-jar", "service.jar"]
