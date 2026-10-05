import ballerina/http;
import ballerina/uuid;
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

final kafka:Producer producer = check new (kafkaBootstrap);

// Order status enum
// CREATED → CONFIRMED → PREPARING → READY → OUT_FOR_DELIVERY → DELIVERED (or CANCELLED)
type OrderStatus "CREATED"|"CONFIRMED"|"PREPARING"|"READY"|"OUT_FOR_DELIVERY"|"DELIVERED"|"CANCELLED";

type OrderItem record {|
    string menuItemId;
    string name;
    int quantity;
    decimal price;
|};

type Order record {|
    string id;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal total;
    OrderStatus status;
    string? driverId;
    string? deliveryId;
    string? paymentId;
    string createdAt;
    string updatedAt;
|};

type OrderRequest record {|
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal total;
|};

// Valid state transitions
function isValidTransition(OrderStatus current, OrderStatus next) returns boolean {
    match current {
        "CREATED" => {
            return next == "CONFIRMED" || next == "CANCELLED";
        }
        "CONFIRMED" => {
            return next == "PREPARING" || next == "CANCELLED";
        }
        "PREPARING" => {
            return next == "READY" || next == "CANCELLED";
        }
        "READY" => {
            return next == "OUT_FOR_DELIVERY" || next == "CANCELLED";
        }
        "OUT_FOR_DELIVERY" => {
            return next == "DELIVERED";
        }
        _ => {
            return false;
        }
    }
}

// HTTP Service
service /orders on new http:Listener(8081) {

    // Create a new order
    resource function post .(OrderRequest req) returns json|http:InternalServerError {
        string orderId = uuid:createType1AsString();
        string now = time:utcToString(time:utcNow());
        Order 'order = {
            id: orderId,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: req.items,
            total: req.total,
            status: "CREATED",
            driverId: (),
            deliveryId: (),
            paymentId: (),
            createdAt: now,
            updatedAt: now
        };

        map<json> doc = orderToDoc('order);
        mongodb:Error? insertResult = ordersCol->insertOne(doc);
        if insertResult is mongodb:Error {
            log:printError("Failed to insert order", insertResult);
            return <http:InternalServerError>{body: {message: "Failed to create order"}};
        }

        // Emit Kafka event: orders.created
        json event = {
            orderId: orderId,
            eventType: "ORDER_CREATED",
            timestamp: now,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: req.items.toJson(),
            total: req.total
        };
        kafka:Error? sendResult = producer->send({
            topic: "orders.created",
            key: orderId.toBytes(),
            value: event.toJsonString().toBytes()
        });
        if sendResult is kafka:Error {
            log:printError("Failed to send Kafka event", sendResult);
        }

        log:printInfo("Order created: " + orderId);
        return {orderId: orderId, status: "CREATED"};
    }

    // Get order by ID
    resource function get [string orderId]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = ordersCol->findOne({"id": orderId});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Order not found", orderId: orderId}};
        }
        return result;
    }

    // Get all orders
    resource function get .() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = ordersCol->find({});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch orders"}};
        }
        json[] orders = [];
        error? e = result.forEach(function(map<json> doc) {
            orders.push(doc);
        });
        if e is error {
            log:printError("Error iterating orders", e);
        }
        return orders;
    }

    // Get orders by customer
    resource function get customer/[string customerId]() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = ordersCol->find({"customerId": customerId});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch orders"}};
        }
        json[] orders = [];
        error? e = result.forEach(function(map<json> doc) {
            orders.push(doc);
        });
        if e is error {
            log:printError("Error iterating orders", e);
        }
        return orders;
    }

    // Update order status (state machine transition)
    resource function put [string orderId]/status(json statusReq) returns json|http:NotFound|http:BadRequest|http:InternalServerError {
        map<json>|mongodb:Error? existing = ordersCol->findOne({"id": orderId});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Order not found"}};
        }

        string|error currentStatusStr = existing["status"].ensureType();
        string|error nextStatusStr = statusReq.status.ensureType();
        if currentStatusStr is error || nextStatusStr is error {
            return <http:BadRequest>{body: {message: "Invalid status"}};
        }

        OrderStatus|error currentStatus = currentStatusStr.ensureType();
        OrderStatus|error nextStatus = nextStatusStr.ensureType();
        if currentStatus is error || nextStatus is error {
            return <http:BadRequest>{body: {message: "Invalid status value"}};
        }

        if !isValidTransition(currentStatus, nextStatus) {
            return <http:BadRequest>{body: {
                message: "Invalid state transition",
                currentStatus: currentStatus,
                requestedStatus: nextStatus
            }};
        }

        string now = time:utcToString(time:utcNow());
        mongodb:UpdateResult|mongodb:Error updateResult = ordersCol->updateOne(
            {"id": orderId},
            {"set": {"status": nextStatus, "updatedAt": now}}
        );
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to update status"}};
        }

        // Emit status change event
        json statusEvent = {
            orderId: orderId,
            eventType: "ORDER_STATUS_CHANGED",
            previousStatus: currentStatus,
            newStatus: nextStatus,
            timestamp: now
        };
        kafka:Error? sendResult = producer->send({
            topic: "orders.status",
            key: orderId.toBytes(),
            value: statusEvent.toJsonString().toBytes()
        });
        if sendResult is kafka:Error {
            log:printError("Failed to send status event", sendResult);
        }

        return {orderId: orderId, previousStatus: currentStatus, status: nextStatus};
    }

    // Cancel order
    resource function put [string orderId]/cancel() returns json|http:NotFound|http:BadRequest|http:InternalServerError {
        map<json>|mongodb:Error? existing = ordersCol->findOne({"id": orderId});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Order not found"}};
        }

        string|error currentStatusStr = existing["status"].ensureType();
        if currentStatusStr is error {
            return <http:BadRequest>{body: {message: "Invalid current status"}};
        }

        OrderStatus|error currentStatus = currentStatusStr.ensureType();
        if currentStatus is error {
            return <http:BadRequest>{body: {message: "Invalid status value"}};
        }

        if !isValidTransition(currentStatus, "CANCELLED") {
            return <http:BadRequest>{body: {
                message: "Order cannot be cancelled in current state",
                currentStatus: currentStatus
            }};
        }

        string now = time:utcToString(time:utcNow());
        mongodb:UpdateResult|mongodb:Error updateResult = ordersCol->updateOne(
            {"id": orderId},
            {"set": {"status": "CANCELLED", "updatedAt": now}}
        );
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to cancel order"}};
        }

        json cancelEvent = {
            orderId: orderId,
            eventType: "ORDER_CANCELLED",
            timestamp: now
        };
        kafka:Error? sendResult = producer->send({
            topic: "orders.status",
            key: orderId.toBytes(),
            value: cancelEvent.toJsonString().toBytes()
        });
        if sendResult is kafka:Error {
            log:printError("Failed to send cancel event", sendResult);
        }

        return {orderId: orderId, status: "CANCELLED"};
    }
}

// Kafka Consumer: Listen for payment and delivery events to auto-update order status
listener kafka:Listener orderEventsListener = new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["payments.completed", "delivery.assigned", "delivery.completed"]
});

service on orderEventsListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var record in records {
            string value = check string:fromBytes(record.value);
            json payload = check value.fromJsonString();
            string eventType = check payload.eventType.ensureType();
            string orderId = check payload.orderId.ensureType();
            string now = time:utcToString(time:utcNow());

            match eventType {
                "PAYMENT_COMPLETED" => {
                    // Move order from CREATED -> CONFIRMED
                    string|error paymentId = payload.paymentId.ensureType();
                    map<json> updateFields = {"status": "CONFIRMED", "updatedAt": now};
                    if paymentId is string {
                        updateFields["paymentId"] = paymentId;
                    }
                    mongodb:UpdateResult|mongodb:Error updateResult = ordersCol->updateOne(
                        {"id": orderId},
                        {"set": updateFields}
                    );
                    if updateResult is mongodb:Error {
                        log:printError("Failed to update order for payment", updateResult);
                    } else {
                        log:printInfo("Order confirmed after payment: " + orderId);
                    }
                }
                "DELIVERY_ASSIGNED" => {
                    // Update order with delivery info -> OUT_FOR_DELIVERY
                    string|error deliveryId = payload.deliveryId.ensureType();
                    string|error driverId = payload.driverId.ensureType();
                    map<json> updateFields = {"status": "OUT_FOR_DELIVERY", "updatedAt": now};
                    if deliveryId is string {
                        updateFields["deliveryId"] = deliveryId;
                    }
                    if driverId is string {
                        updateFields["driverId"] = driverId;
                    }
                    mongodb:UpdateResult|mongodb:Error updateResult = ordersCol->updateOne(
                        {"id": orderId},
                        {"set": updateFields}
                    );
                    if updateResult is mongodb:Error {
                        log:printError("Failed to update order for delivery", updateResult);
                    } else {
                        log:printInfo("Order out for delivery: " + orderId);
                    }
                }
                "DELIVERY_COMPLETED" => {
                    // Mark order as DELIVERED
                    mongodb:UpdateResult|mongodb:Error updateResult = ordersCol->updateOne(
                        {"id": orderId},
                        {"set": {"status": "DELIVERED", "updatedAt": now}}
                    );
                    if updateResult is mongodb:Error {
                        log:printError("Failed to mark order as delivered", updateResult);
                    } else {
                        log:printInfo("Order delivered: " + orderId);
                    }
                }
            }
        }
    }
}

function orderToDoc(Order o) returns map<json> {
    return {
        "id": o.id,
        "customerId": o.customerId,
        "restaurantId": o.restaurantId,
        "items": o.items.toJson(),
        "total": o.total,
        "status": o.status,
        "driverId": o.driverId is () ? null : o.driverId,
        "deliveryId": o.deliveryId is () ? null : o.deliveryId,
        "paymentId": o.paymentId is () ? null : o.paymentId,
        "createdAt": o.createdAt,
        "updatedAt": o.updatedAt
    };
}
