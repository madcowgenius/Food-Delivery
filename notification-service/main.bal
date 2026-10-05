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
final mongodb:Collection notificationsCol = check db->getCollection("notifications");

// Record types
type NotificationChannel "SMS"|"EMAIL"|"PUSH";

type Notification record {|
    string id;
    string recipientId;
    string recipientType;
    NotificationChannel channel;
    string subject;
    string message;
    string eventType;
    string status;
    string createdAt;
|};

type NotificationRequest record {|
    string recipientId;
    string recipientType;
    string channel;
    string subject;
    string message;
|};

// HTTP Service for manual notifications
service /notifications on new http:Listener(8086) {

    // Send notification manually
    resource function post .(NotificationRequest req) returns json|http:InternalServerError {
        string id = uuid:createType1AsString();
        string now = time:utcToString(time:utcNow());

        map<json> doc = {
            "id": id,
            "recipientId": req.recipientId,
            "recipientType": req.recipientType,
            "channel": req.channel,
            "subject": req.subject,
            "message": req.message,
            "eventType": "MANUAL",
            "status": "SENT",
            "createdAt": now
        };

        mongodb:Error? insertResult = notificationsCol->insertOne(doc);
        if insertResult is mongodb:Error {
            log:printError("Failed to store notification", insertResult);
            return <http:InternalServerError>{body: {message: "Failed to send notification"}};
        }

        log:printInfo(string `Notification sent to ${req.recipientId} via ${req.channel}: ${req.subject}`);
        return {
            id: id,
            status: "SENT",
            recipientId: req.recipientId,
            channel: req.channel,
            subject: req.subject,
            message: req.message,
            timestamp: now
        };
    }

    // Get all notifications
    resource function get .() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = notificationsCol->find({});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch notifications"}};
        }
        json[] notifications = [];
        error? e = result.forEach(function(map<json> doc) {
            notifications.push(doc);
        });
        if e is error {
            log:printError("Error iterating notifications", e);
        }
        return notifications;
    }

    // Get notifications for a specific recipient
    resource function get recipient/[string recipientId]() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = notificationsCol->find({"recipientId": recipientId});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch notifications"}};
        }
        json[] notifications = [];
        error? e = result.forEach(function(map<json> doc) {
            notifications.push(doc);
        });
        if e is error {
            log:printError("Error iterating notifications", e);
        }
        return notifications;
    }

    // Get notification by ID
    resource function get [string id]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = notificationsCol->findOne({"id": id});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Notification not found"}};
        }
        return result;
    }
}

// Helper function to store a notification from Kafka events
function storeNotification(string recipientId, string recipientType, string channel,
        string subject, string message, string eventType) {
    string id = uuid:createType1AsString();
    string now = time:utcToString(time:utcNow());

    map<json> doc = {
        "id": id,
        "recipientId": recipientId,
        "recipientType": recipientType,
        "channel": channel,
        "subject": subject,
        "message": message,
        "eventType": eventType,
        "status": "SENT",
        "createdAt": now
    };

    mongodb:Error? insertResult = notificationsCol->insertOne(doc);
    if insertResult is mongodb:Error {
        log:printError("Failed to store notification from event", insertResult);
    } else {
        log:printInfo(string `[${channel}] Notification to ${recipientType}:${recipientId} - ${subject}`);
    }
}

// Kafka Consumer: Listen to all major events and emit notifications
listener kafka:Listener notificationEventsListener = new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: ["orders.created", "orders.status", "payments.completed", "delivery.assigned", "delivery.completed"]
});

service on notificationEventsListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var record in records {
            string value = check string:fromBytes(record.value);
            json payload = check value.fromJsonString();
            string eventType = check payload.eventType.ensureType();

            match eventType {
                "ORDER_CREATED" => {
                    string orderId = check payload.orderId.ensureType();
                    string customerId = check payload.customerId.ensureType();
                    string restaurantId = check payload.restaurantId.ensureType();

                    // Notify customer
                    storeNotification(customerId, "CUSTOMER", "PUSH",
                        "Order Placed",
                        string `Your order ${orderId} has been placed successfully!`,
                        eventType);

                    // Notify restaurant
                    storeNotification(restaurantId, "RESTAURANT", "PUSH",
                        "New Order Received",
                        string `New order ${orderId} received. Please confirm.`,
                        eventType);
                }
                "ORDER_STATUS_CHANGED" => {
                    string orderId = check payload.orderId.ensureType();
                    string newStatus = check payload.newStatus.ensureType();

                    storeNotification("system", "CUSTOMER", "PUSH",
                        "Order Status Update",
                        string `Order ${orderId} status changed to ${newStatus}.`,
                        eventType);
                }
                "ORDER_CANCELLED" => {
                    string orderId = check payload.orderId.ensureType();

                    storeNotification("system", "CUSTOMER", "EMAIL",
                        "Order Cancelled",
                        string `Order ${orderId} has been cancelled.`,
                        eventType);
                }
                "PAYMENT_COMPLETED" => {
                    string orderId = check payload.orderId.ensureType();
                    string|error customerId = payload.customerId.ensureType();
                    string custId = customerId is string ? customerId : "unknown";

                    storeNotification(custId, "CUSTOMER", "SMS",
                        "Payment Confirmed",
                        string `Payment for order ${orderId} has been confirmed.`,
                        eventType);
                }
                "DELIVERY_ASSIGNED" => {
                    string orderId = check payload.orderId.ensureType();
                    string|error driverId = payload.driverId.ensureType();
                    string dId = driverId is string ? driverId : "unknown";

                    // Notify driver
                    storeNotification(dId, "DRIVER", "PUSH",
                        "New Delivery Assignment",
                        string `You have been assigned to deliver order ${orderId}.`,
                        eventType);

                    // Notify customer
                    storeNotification("system", "CUSTOMER", "PUSH",
                        "Driver Assigned",
                        string `A driver has been assigned for your order ${orderId}.`,
                        eventType);
                }
                "DELIVERY_COMPLETED" => {
                    string orderId = check payload.orderId.ensureType();

                    storeNotification("system", "CUSTOMER", "PUSH",
                        "Order Delivered",
                        string `Your order ${orderId} has been delivered! Enjoy your meal!`,
                        eventType);

                    storeNotification("system", "RESTAURANT", "EMAIL",
                        "Delivery Completed",
                        string `Order ${orderId} has been delivered successfully.`,
                        eventType);
                }
            }
        }
    }
}
