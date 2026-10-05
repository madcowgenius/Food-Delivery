

import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

// MongoDB configuration
configurable string mongoHost = "mongo";
configurable int mongoPort = 27017;
configurable string kafkaBootstrap = "kafka:19092";

final mongodb:Client mongoClient = check new ({
    connection: {
        serverAddress: {
            host: mongoHost,
            port: mongoPort
        }
    }
});

final mongodb:Database db = check mongoClient->getDatabase("food_delivery");
final mongodb:Collection ordersCol = check db->getCollection("orders");
final mongodb:Collection paymentsCol = check db->getCollection("payments");
final mongodb:Collection deliveriesCol = check db->getCollection("deliveries");
final mongodb:Collection driversCol = check db->getCollection("drivers");
final mongodb:Collection customersCol = check db->getCollection("customers");
final mongodb:Collection restaurantsCol = check db->getCollection("restaurants");
final mongodb:Collection notificationsCol = check db->getCollection("notifications");
final mongodb:Collection eventsCol = check db->getCollection("admin_events");

// HTTP Service
service /admin on new http:Listener(8087) {

    // Health check
    resource function get health() returns json {
        return {
            'service: "food-delivery-platform",
            status: "UP",
            timestamp: time:utcToString(time:utcNow()),
            managedServices: [
                "customer-service:8082",
                "restaurant-service:8083",
                "order-service:8081",
                "payment-service:8084",
                "delivery-service:8085",
                "notification-service:8086"
            ]
        };
    }

    // Dashboard overview - aggregated statistics
    resource function get dashboard() returns json|http:InternalServerError {
        int totalOrders = getCollectionCount(ordersCol);
        int totalCustomers = getCollectionCount(customersCol);
        int totalRestaurants = getCollectionCount(restaurantsCol);
        int totalDrivers = getCollectionCount(driversCol);
        int totalDeliveries = getCollectionCount(deliveriesCol);
        int totalPayments = getCollectionCount(paymentsCol);
        int totalNotifications = getCollectionCount(notificationsCol);

        return {
            timestamp: time:utcToString(time:utcNow()),
            overview: {
                totalOrders: totalOrders,
                totalCustomers: totalCustomers,
                totalRestaurants: totalRestaurants,
                totalDrivers: totalDrivers,
                totalDeliveries: totalDeliveries,
                totalPayments: totalPayments,
                totalNotifications: totalNotifications
            }
        };
    }

    // Order statistics report
    resource function get reports/orders() returns json|http:InternalServerError {
        // Count orders by status
        int createdCount = getFilteredCount(ordersCol, {"status": "CREATED"});
        int confirmedCount = getFilteredCount(ordersCol, {"status": "CONFIRMED"});
        int preparingCount = getFilteredCount(ordersCol, {"status": "PREPARING"});
        int readyCount = getFilteredCount(ordersCol, {"status": "READY"});
        int outForDeliveryCount = getFilteredCount(ordersCol, {"status": "OUT_FOR_DELIVERY"});
        int deliveredCount = getFilteredCount(ordersCol, {"status": "DELIVERED"});
        int cancelledCount = getFilteredCount(ordersCol, {"status": "CANCELLED"});

        // Calculate total revenue from completed orders
        decimal totalRevenue = getTotalRevenue();

        return {
            reportType: "ORDER_STATISTICS",
            generatedAt: time:utcToString(time:utcNow()),
            ordersByStatus: {
                created: createdCount,
                confirmed: confirmedCount,
                preparing: preparingCount,
                ready: readyCount,
                outForDelivery: outForDeliveryCount,
                delivered: deliveredCount,
                cancelled: cancelledCount
            },
            totalRevenue: totalRevenue,
            totalOrders: createdCount + confirmedCount + preparingCount + readyCount +
                outForDeliveryCount + deliveredCount + cancelledCount
        };
    }

    // Delivery performance report
    resource function get reports/deliveries() returns json|http:InternalServerError {
        int assignedCount = getFilteredCount(deliveriesCol, {"status": "ASSIGNED"});
        int pickedUpCount = getFilteredCount(deliveriesCol, {"status": "PICKED_UP"});
        int inTransitCount = getFilteredCount(deliveriesCol, {"status": "IN_TRANSIT"});
        int deliveredCount = getFilteredCount(deliveriesCol, {"status": "DELIVERED"});

        int totalDrivers = getCollectionCount(driversCol);
        int availableDrivers = getFilteredCount(driversCol, {"available": true});
        int busyDrivers = getFilteredCount(driversCol, {"available": false});

        return {
            reportType: "DELIVERY_PERFORMANCE",
            generatedAt: time:utcToString(time:utcNow()),
            deliveriesByStatus: {
                assigned: assignedCount,
                pickedUp: pickedUpCount,
                inTransit: inTransitCount,
                delivered: deliveredCount
            },
            driverStatistics: {
                totalDrivers: totalDrivers,
                availableDrivers: availableDrivers,
                busyDrivers: busyDrivers
            }
        };
    }

    // Payment statistics report
    resource function get reports/payments() returns json|http:InternalServerError {
        int completedPayments = getFilteredCount(paymentsCol, {"status": "COMPLETED"});
        int refundedPayments = getFilteredCount(paymentsCol, {"status": "REFUNDED"});
        int failedPayments = getFilteredCount(paymentsCol, {"status": "FAILED"});

        return {
            reportType: "PAYMENT_STATISTICS",
            generatedAt: time:utcToString(time:utcNow()),
            paymentsByStatus: {
                completed: completedPayments,
                refunded: refundedPayments,
                failed: failedPayments
            },
            totalPayments: completedPayments + refundedPayments + failedPayments
        };
    }

    // Restaurant statistics report
    resource function get reports/restaurants() returns json|http:InternalServerError {
        int totalRestaurants = getCollectionCount(restaurantsCol);
        int openRestaurants = getFilteredCount(restaurantsCol, {"isOpen": true});
        int closedRestaurants = getFilteredCount(restaurantsCol, {"isOpen": false});

        return {
            reportType: "RESTAURANT_STATISTICS",
            generatedAt: time:utcToString(time:utcNow()),
            totalRestaurants: totalRestaurants,
            openRestaurants: openRestaurants,
            closedRestaurants: closedRestaurants
        };
    }

    // Get all collected events (audit log)
    resource function get events() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = eventsCol->find({});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch events"}};
        }
        json[] events = [];
        error? e = result.forEach(function(map<json> doc) {
            events.push(doc);
        });
        if e is error {
            log:printError("Error iterating events", e);
        }
        return events;
    }
}

// Kafka Consumer: Collect all events for audit and reporting
listener kafka:Listener adminEventsListener = new (kafkaBootstrap, {
    groupId: "admin-service-group",
    topics: ["orders.created", "orders.status", "payments.completed", "payments.refunded",
             "delivery.assigned", "delivery.completed"]
});

service on adminEventsListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var record in records {
            string value = check string:fromBytes(record.value);
            json payload = check value.fromJsonString();
            string eventType = check payload.eventType.ensureType();
            string now = time:utcToString(time:utcNow());

            map<json> eventDoc = {
                "eventType": eventType,
                "topic": record.topic,
                "payload": value,
                "receivedAt": now
            };

            mongodb:Error? insertResult = eventsCol->insertOne(eventDoc);
            if insertResult is mongodb:Error {
                log:printError("Failed to store admin event", insertResult);
            } else {
                log:printInfo(string `Admin event logged: ${eventType}`);
            }
        }
    }
}

// Helper functions
function getCollectionCount(mongodb:Collection col) returns int {
    stream<map<json>, error?>|mongodb:Error result = col->find({});
    if result is mongodb:Error {
        return 0;
    }
    int count = 0;
    error? e = result.forEach(function(map<json> doc) {
        count = count + 1;
    });
    if e is error {
        log:printError("Error counting documents", e);
    }
    return count;
}

function getFilteredCount(mongodb:Collection col, map<json> filter) returns int {
    stream<map<json>, error?>|mongodb:Error result = col->find(filter);
    if result is mongodb:Error {
        return 0;
    }
    int count = 0;
    error? e = result.forEach(function(map<json> doc) {
        count = count + 1;
    });
    if e is error {
        log:printError("Error counting filtered documents", e);
    }
    return count;
}

function getTotalRevenue() returns decimal {
    stream<map<json>, error?>|mongodb:Error result = ordersCol->find({"status": "DELIVERED"});
    if result is mongodb:Error {
        return 0d;
    }
    decimal total = 0d;
    error? e = result.forEach(function(map<json> doc) {
        decimal|error amount = doc["total"].ensureType();
        if amount is decimal {
            total = total + amount;
        }
    });
    if e is error {
        log:printError("Error calculating revenue", e);
    }
    return total;
}
