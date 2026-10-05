import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

// ──────────────────────────────────────────────
// Configurable Variables
// ──────────────────────────────────────────────
configurable int servicePort = 8083;
configurable string kafkaBootstrapServers = "localhost:9092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string databaseName = "order_db";

// ──────────────────────────────────────────────
// Record Types
// ──────────────────────────────────────────────

# Order status enumeration representing the order lifecycle.
# CREATED → CONFIRMED → PREPARING → READY → OUT_FOR_DELIVERY → DELIVERED (or CANCELLED)
enum OrderStatus {
    CREATED,
    CONFIRMED,
    PREPARING,
    READY,
    OUT_FOR_DELIVERY,
    DELIVERED,
    CANCELLED
}

# Represents an item in an order.
type OrderItem record {|
    string menuItemId;
    string name;
    int quantity;
    float price;
|};

# Request payload for creating a new order.
type OrderRequest record {|
    string customerId;
    string restaurantId;
    OrderItem[] items;
    string deliveryAddress;
    float deliveryLatitude?;
    float deliveryLongitude?;
|};

# Request payload for updating order status.
type StatusUpdateRequest record {|
    string status;
|};

// ──────────────────────────────────────────────
// MongoDB and Kafka Initialization
// ──────────────────────────────────────────────
final mongodb:Client mongoClient;
final mongodb:Database db;
const string ORDERS_COLLECTION = "orders";
final mongodb:Collection ordersCollection;

final kafka:Producer kafkaProducer;

// ──────────────────────────────────────────────
// Kafka Topic Constants
// ──────────────────────────────────────────────
const string TOPIC_ORDERS_CREATED = "orders.created";
const string TOPIC_ORDERS_CONFIRMED = "orders.confirmed";
const string TOPIC_ORDERS_PREPARING = "orders.preparing";
const string TOPIC_ORDERS_READY = "orders.ready";
const string TOPIC_ORDERS_CANCELLED = "orders.cancelled";

function init() returns error? {
    mongoClient = check new ({
        connection: {
            serverAddress: {
                host: mongoHost,
                port: mongoPort
            }
        }
    });
    db = check mongoClient->getDatabase(databaseName);
    ordersCollection = check db->getCollection(ORDERS_COLLECTION);
    kafkaProducer = check new (kafkaBootstrapServers);
}

// Helper function to query a collection and return array of map<json>
isolated function queryCollection(mongodb:Collection collectionRef, map<json> filter = {}) returns map<json>[]|error {
    stream<map<json>, error?> docStream = check collectionRef->find(filter);
    map<json>[] results = check from var doc in docStream select doc;
    check docStream.close();
    return results;
}

// ──────────────────────────────────────────────
// Helper: Calculate total amount from order items
// ──────────────────────────────────────────────
function calculateTotal(OrderItem[] items) returns float {
    float total = 0.0;
    foreach OrderItem item in items {
        total += item.price * <float>item.quantity;
    }
    return total;
}

// ──────────────────────────────────────────────
// Helper: Validate order status transitions
// ──────────────────────────────────────────────
function isValidTransition(string currentStatus, string newStatus) returns boolean {
    if currentStatus == "CREATED" && (newStatus == "CONFIRMED" || newStatus == "CANCELLED") {
        return true;
    } else if currentStatus == "CONFIRMED" && (newStatus == "PREPARING" || newStatus == "CANCELLED") {
        return true;
    } else if currentStatus == "PREPARING" && (newStatus == "READY" || newStatus == "CANCELLED") {
        return true;
    } else if currentStatus == "READY" && (newStatus == "OUT_FOR_DELIVERY" || newStatus == "CANCELLED") {
        return true;
    } else if currentStatus == "OUT_FOR_DELIVERY" && newStatus == "DELIVERED" {
        return true;
    }
    return false;
}

// ──────────────────────────────────────────────
// Helper: Publish event to Kafka topic
// ──────────────────────────────────────────────
function publishOrderEvent(string topic, json payload) returns error? {
    byte[] serializedMsg = payload.toJsonString().toBytes();
    check kafkaProducer->send({
        topic: topic,
        value: serializedMsg
    });
    log:printInfo("Published event", topic = topic);
}

// ──────────────────────────────────────────────
// HTTP REST API Service
// ──────────────────────────────────────────────
service /api/v1 on new http:Listener(servicePort) {

    # Health check endpoint.
    resource function get health() returns json {
        return {"status": "UP", "service": "order-service"};
    }

    # Create a new order.
    # Publishes an `orders.created` event to Kafka.
    # + payload - Order details including items and delivery address
    # + return - Created order or error
    resource function post orders(@http:Payload OrderRequest payload) returns http:Created|http:BadRequest|http:InternalServerError {
        do {
            if payload.items.length() == 0 {
                http:BadRequest badRequest = {
                    body: {"status": "error", "message": "Order must contain at least one item"}
                };
                return badRequest;
            }

            string orderId = uuid:createType1AsString();
            string currentTime = time:utcToString(time:utcNow());
            float totalAmount = calculateTotal(payload.items);

            json[] itemsJson = [];
            foreach OrderItem item in payload.items {
                itemsJson.push({
                    "menuItemId": item.menuItemId,
                    "name": item.name,
                    "quantity": item.quantity,
                    "price": item.price
                });
            }

            map<json> orderDoc = {
                "_id": orderId,
                "customerId": payload.customerId,
                "restaurantId": payload.restaurantId,
                "items": itemsJson,
                "status": "CREATED",
                "totalAmount": totalAmount,
                "deliveryAddress": payload.deliveryAddress,
                "driverId": (),
                "createdAt": currentTime,
                "updatedAt": currentTime
            };

            if payload.deliveryLatitude is float {
                orderDoc["deliveryLatitude"] = <float>payload.deliveryLatitude;
            }
            if payload.deliveryLongitude is float {
                orderDoc["deliveryLongitude"] = <float>payload.deliveryLongitude;
            }

            check ordersCollection->insertOne(orderDoc);
            check publishOrderEvent(TOPIC_ORDERS_CREATED, orderDoc);

            log:printInfo("Order created", orderId = orderId, customerId = payload.customerId);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Order placed successfully",
                    "data": orderDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to create order", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all orders with optional filtering.
    # + status - Optional status filter
    # + return - List of orders
    resource function get orders(string? status) returns json|http:InternalServerError {
        do {
            map<json> filter = {};
            if status is string {
                filter["status"] = status;
            }

            map<json>[] orders = check queryCollection(ordersCollection, filter);

            return {
                "status": "success",
                "count": orders.length(),
                "data": orders
            };
        } on fail error e {
            log:printError("Failed to retrieve orders", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve a single order by ID.
    # + orderId - The unique order identifier
    # + return - Order details or not found
    resource function get orders/[string orderId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": orderId};
            map<json>[] results = check queryCollection(ordersCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Order not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve order", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Update the status of an order (manual trigger from restaurant/admin).
    # Validates state machine transitions and publishes corresponding Kafka events.
    # + orderId - The unique order identifier
    # + payload - New status
    # + return - Updated order or error
    resource function put orders/[string orderId]/status(@http:Payload StatusUpdateRequest payload) returns json|http:BadRequest|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": orderId};
            map<json>[] results = check queryCollection(ordersCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Order not found"}
                };
                return notFound;
            }

            string currentStatus = check results[0]["status"].ensureType();
            string newStatus = payload.status;

            if !isValidTransition(currentStatus, newStatus) {
                http:BadRequest badRequest = {
                    body: {
                        "status": "error",
                        "message": "Invalid status transition from " + currentStatus + " to " + newStatus
                    }
                };
                return badRequest;
            }

            string currentTime = time:utcToString(time:utcNow());
            map<json> updateFields = {
                "status": newStatus,
                "updatedAt": currentTime
            };

            mongodb:Update update = {set: updateFields};
            _ = check ordersCollection->updateOne(filter, update);

            map<json>[] updatedResults = check queryCollection(ordersCollection, filter);
            json updatedOrder = updatedResults[0];

            if newStatus == "CONFIRMED" {
                check publishOrderEvent(TOPIC_ORDERS_CONFIRMED, updatedOrder);
            } else if newStatus == "PREPARING" {
                check publishOrderEvent(TOPIC_ORDERS_PREPARING, updatedOrder);
            } else if newStatus == "READY" {
                check publishOrderEvent(TOPIC_ORDERS_READY, updatedOrder);
            } else if newStatus == "CANCELLED" {
                check publishOrderEvent(TOPIC_ORDERS_CANCELLED, updatedOrder);
            }

            log:printInfo("Order status updated", orderId = orderId, previousStatus = currentStatus, newStatus = newStatus);

            return {
                "status": "success",
                "message": "Order status updated to " + newStatus,
                "data": updatedOrder
            };
        } on fail error e {
            log:printError("Failed to update order status", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all orders for a specific customer.
    # + customerId - The unique customer identifier
    # + return - List of customer orders
    resource function get orders/customer/[string customerId]() returns json|http:InternalServerError {
        do {
            map<json> filter = {"customerId": customerId};
            map<json>[] orders = check queryCollection(ordersCollection, filter);

            return {
                "status": "success",
                "count": orders.length(),
                "data": orders
            };
        } on fail error e {
            log:printError("Failed to retrieve customer orders", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all orders for a specific restaurant.
    # + restaurantId - The unique restaurant identifier
    # + return - List of restaurant orders
    resource function get orders/restaurant/[string restaurantId]() returns json|http:InternalServerError {
        do {
            map<json> filter = {"restaurantId": restaurantId};
            map<json>[] orders = check queryCollection(ordersCollection, filter);

            return {
                "status": "success",
                "count": orders.length(),
                "data": orders
            };
        } on fail error e {
            log:printError("Failed to retrieve restaurant orders", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }
}

// ──────────────────────────────────────────────
// Kafka Consumer: Listen for Payment & Delivery events
// Automatically updates order status based on events
// ──────────────────────────────────────────────
listener kafka:Listener orderKafkaListener = new (kafkaBootstrapServers, {
    groupId: "order-service-group",
    topics: ["payments.completed", "payments.failed", "delivery.assigned", "delivery.completed"],
    pollingInterval: 1
});

service on orderKafkaListener {

    # Handles incoming Kafka events for order lifecycle management.
    # - payments.completed  → Updates order to CONFIRMED
    # - payments.failed     → Updates order to CANCELLED
    # - delivery.assigned   → Updates order to OUT_FOR_DELIVERY
    # - delivery.completed  → Updates order to DELIVERED
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            do {
                string messageStr = check string:fromBytes(rec.value);
                json message = check messageStr.fromJsonString();
                string topic = rec.offset.partition.topic;

                log:printInfo("Received Kafka event", topic = topic);

                string orderId = check message.orderId.ensureType();
                string currentTime = time:utcToString(time:utcNow());

                if topic == "payments.completed" {
                    map<json> filter = {"_id": orderId};
                    map<json> updateFields = {
                        "status": "CONFIRMED",
                        "updatedAt": currentTime
                    };
                    mongodb:Update update = {set: updateFields};
                    _ = check ordersCollection->updateOne(filter, update);

                    map<json>[] results = check queryCollection(ordersCollection, filter);
                    if results.length() > 0 {
                        check publishOrderEvent(TOPIC_ORDERS_CONFIRMED, results[0]);
                    }

                    log:printInfo("Order confirmed after payment", orderId = orderId);

                } else if topic == "payments.failed" {
                    map<json> filter = {"_id": orderId};
                    map<json> updateFields = {
                        "status": "CANCELLED",
                        "cancelReason": "Payment failed",
                        "updatedAt": currentTime
                    };
                    mongodb:Update update = {set: updateFields};
                    _ = check ordersCollection->updateOne(filter, update);

                    map<json>[] results = check queryCollection(ordersCollection, filter);
                    if results.length() > 0 {
                        check publishOrderEvent(TOPIC_ORDERS_CANCELLED, results[0]);
                    }

                    log:printInfo("Order cancelled due to payment failure", orderId = orderId);

                } else if topic == "delivery.assigned" {
                    string driverId = check message.driverId.ensureType();
                    map<json> filter = {"_id": orderId};
                    map<json> updateFields = {
                        "status": "OUT_FOR_DELIVERY",
                        "driverId": driverId,
                        "updatedAt": currentTime
                    };
                    mongodb:Update update = {set: updateFields};
                    _ = check ordersCollection->updateOne(filter, update);

                    log:printInfo("Order out for delivery", orderId = orderId, driverId = driverId);

                } else if topic == "delivery.completed" {
                    map<json> filter = {"_id": orderId};
                    map<json> updateFields = {
                        "status": "DELIVERED",
                        "deliveredAt": currentTime,
                        "updatedAt": currentTime
                    };
                    mongodb:Update update = {set: updateFields};
                    _ = check ordersCollection->updateOne(filter, update);

                    log:printInfo("Order delivered successfully", orderId = orderId);
                }
            } on fail error e {
                log:printError("Error processing Kafka event", 'error = e);
            }
        }
    }
}
