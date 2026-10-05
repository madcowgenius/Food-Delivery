FROM ballerina/ballerina:2201.13.5 AS build
WORKDIR /app
COPY Ballerina.toml .
COPY main.bal .
RUN bal build

FROM ballerina/ballerina-runtime:2201.13.5
WORKDIR /app
COPY --from=build /app/target/bin/*.jar .
EXPOSE 8082
CMD ["java", "-jar", "customer_service.jar"]
