import ballerina/http;

service /admin on new http:Listener(8087) {
    resource function get /health() returns json {
        return {
            service: "food-delivery-platform",
            status: "UP",
            managedServices: ["customer", "restaurant", "order", "payment", "delivery", "notification"]
        };
    }

    resource function get /reports() returns json {
        return {
            generated: true,
            message: "Operational report aggregation is ready for event-backed metrics"
        };
    }
}
