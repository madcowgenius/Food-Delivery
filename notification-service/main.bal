import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

// ──────────────────────────────────────────────
// Configurable Variables
// ──────────────────────────────────────────────
configurable int servicePort = 8086;
configurable string kafkaBootstrapServers = "localhost:9092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string databaseName = "notification_db";

// ──────────────────────────────────────────────
// Record Types
// ──────────────────────────────────────────────

# Notification channels supported by the system.
enum NotificationChannel {
    SMS,
    EMAIL,
    PUSH
}

# Recipient category.
enum RecipientType {
    CUSTOMER,
    RESTAURANT,
    DRIVER,
    ADMIN
}

# Represents a logged multi-channel notification.
type Notification record {|
    string _id;
    string recipientId;
    string recipientType; // CUSTOMER, RESTAURANT, DRIVER, ADMIN
    string recipientContact;
    string channel; // SMS, EMAIL, PUSH
    string eventType;
    string title;
    string message;
    string status; // SENT, DELIVERED, READ
    string? orderId;
    string timestamp;
|};

# Request payload for manual notification dispatch.
type NotificationRequest record {|
    string recipientId;
    string recipientType;
    string recipientContact;
    string channel;
    string title;
    string message;
    string? orderId = ();
|};

// ──────────────────────────────────────────────
// MongoDB and Kafka Initialization
// ──────────────────────────────────────────────
final mongodb:Client mongoClient;
final mongodb:Database db;
const string NOTIFICATIONS_COLLECTION = "notifications";
final mongodb:Collection notificationsCollection;

final kafka:Producer kafkaProducer;

const string TOPIC_NOTIFICATIONS_SEND = "notifications.send";

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
    notificationsCollection = check db->getCollection(NOTIFICATIONS_COLLECTION);
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
// Helper: Disseminate and Record Multi-Channel Alert
// ──────────────────────────────────────────────
function recordAndSendAlert(
    string recipientId,
    string recipientType,
    string recipientContact,
    string channel,
    string eventType,
    string title,
    string messageText,
    string? orderId = ()
) returns error? {
    string notifId = uuid:createType1AsString();
    string currentTime = time:utcToString(time:utcNow());

    map<json> notifDoc = {
        "_id": notifId,
        "recipientId": recipientId,
        "recipientType": recipientType,
        "recipientContact": recipientContact,
        "channel": channel,
        "eventType": eventType,
        "title": title,
        "message": messageText,
        "status": "SENT",
        "orderId": orderId,
        "timestamp": currentTime
    };

    check notificationsCollection->insertOne(notifDoc);

    // Simulate multi-channel delivery transmission log
    log:printInfo("Multi-channel notification dispatched",
        channel = channel,
        recipient = recipientType,
        contact = recipientContact,
        title = title
    );
}

// ──────────────────────────────────────────────
// HTTP REST API Service
// ──────────────────────────────────────────────
service /api/v1 on new http:Listener(servicePort) {

    # Health check endpoint.
    resource function get health() returns json {
        return {"status": "UP", "service": "notification-service"};
    }

    # Dispatch a notification manually across specified channel.
    # + payload - Notification details
    # + return - Created notification or error
    resource function post notifications(@http:Payload NotificationRequest payload) returns http:Created|http:InternalServerError {
        do {
            string notifId = uuid:createType1AsString();
            string currentTime = time:utcToString(time:utcNow());

            map<json> notifDoc = {
                "_id": notifId,
                "recipientId": payload.recipientId,
                "recipientType": payload.recipientType,
                "recipientContact": payload.recipientContact,
                "channel": payload.channel,
                "eventType": "manual.notification",
                "title": payload.title,
                "message": payload.message,
                "status": "SENT",
                "orderId": payload.orderId,
                "timestamp": currentTime
            };

            check notificationsCollection->insertOne(notifDoc);

            log:printInfo("Manual notification created", id = notifId, recipient = payload.recipientId, channel = payload.channel);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Notification dispatched successfully",
                    "data": notifDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to dispatch notification", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all notifications with optional channel/type filters.
    # + channel - Optional channel filter (SMS, EMAIL, PUSH)
    # + recipientType - Optional recipient type filter (CUSTOMER, RESTAURANT, DRIVER)
    # + return - List of notifications
    resource function get notifications(string? channel, string? recipientType) returns json|http:InternalServerError {
        do {
            map<json> filter = {};
            if channel is string {
                filter["channel"] = channel;
            }
            if recipientType is string {
                filter["recipientType"] = recipientType;
            }

            map<json>[] notifs = check queryCollection(notificationsCollection, filter);

            return {
                "status": "success",
                "count": notifs.length(),
                "data": notifs
            };
        } on fail error e {
            log:printError("Failed to retrieve notifications", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve notification by ID.
    # + id - Unique notification ID
    # + return - Notification record
    resource function get notifications/[string id]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": id};
            map<json>[] results = check queryCollection(notificationsCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Notification not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve notification", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve notifications for a specific recipient (customer, restaurant, or driver).
    # + recipientId - Recipient unique identifier
    # + return - List of recipient's notifications
    resource function get notifications/recipient/[string recipientId]() returns json|http:InternalServerError {
        do {
            map<json> filter = {"recipientId": recipientId};
            map<json>[] notifs = check queryCollection(notificationsCollection, filter);

            return {
                "status": "success",
                "recipientId": recipientId,
                "count": notifs.length(),
                "data": notifs
            };
        } on fail error e {
            log:printError("Failed to retrieve notifications for recipient", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all notifications related to an order ID.
    # + orderId - Order identifier
    # + return - List of notifications for order
    resource function get notifications/'order/[string orderId]() returns json|http:InternalServerError {
        do {
            map<json> filter = {"orderId": orderId};
            map<json>[] notifs = check queryCollection(notificationsCollection, filter);

            return {
                "status": "success",
                "orderId": orderId,
                "count": notifs.length(),
                "data": notifs
            };
        } on fail error e {
            log:printError("Failed to retrieve notifications for order", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Mark a notification as READ.
    # + id - Notification unique identifier
    # + return - Updated notification status
    resource function put notifications/[string id]/read() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": id};
            mongodb:Update update = {set: {"status": "READ"}};
            mongodb:UpdateResult res = check notificationsCollection->updateOne(filter, update);

            if res.matchedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Notification not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "message": "Notification marked as read"
            };
        } on fail error e {
            log:printError("Failed to mark notification as read", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }
}

// ──────────────────────────────────────────────
// Kafka Multi-Topic Consumer: Listen to Platform Events
// ──────────────────────────────────────────────
listener kafka:Listener notificationKafkaListener = new (kafkaBootstrapServers, {
    groupId: "notification-service-group",
    topics: [
        "orders.created",
        "orders.confirmed",
        "orders.preparing",
        "orders.ready",
        "orders.cancelled",
        "payments.completed",
        "payments.failed",
        "delivery.assigned",
        "delivery.picked_up",
        "delivery.completed",
        "notifications.send"
    ],
    pollingInterval: 1
});

service on notificationKafkaListener {

    # Consumes lifecycle events across the platform and disseminates targeted alerts
    # to Customers, Restaurants, and Drivers via SMS, Email, and Push notifications.
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            do {
                string messageStr = check string:fromBytes(rec.value);
                json message = check messageStr.fromJsonString();
                string topic = rec.offset.partition.topic;

                log:printInfo("Notification service received event", topic = topic);

                if topic == "orders.created" {
                    string orderId = check message._id.ensureType();
                    string customerId = check message.customerId.ensureType();
                    string restaurantId = check message.restaurantId.ensureType();

                    // Alert Customer via Push and SMS
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "customer@foodplatform.na", "PUSH",
                        topic, "Order Received!",
                        "Your order #" + orderId + " has been placed and sent to the kitchen.",
                        orderId
                    );
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "+264 81 100 2000", "SMS",
                        topic, "Order Placed",
                        "Thanks for your order! Order #" + orderId + " is awaiting kitchen confirmation.",
                        orderId
                    );

                    // Alert Restaurant via Push
                    check recordAndSendAlert(
                        restaurantId, "RESTAURANT", "orders@restaurant.na", "PUSH",
                        topic, "New Order Incoming!",
                        "New order #" + orderId + " received. Please confirm and begin preparation.",
                        orderId
                    );

                } else if topic == "orders.confirmed" {
                    string orderId = check message._id.ensureType();
                    string customerId = check message.customerId.ensureType();

                    // Customer Email and SMS
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "customer@foodplatform.na", "EMAIL",
                        topic, "Order Confirmed!",
                        "Great news! Your order #" + orderId + " has been confirmed and accepted.",
                        orderId
                    );

                } else if topic == "orders.preparing" {
                    string orderId = check message._id.ensureType();
                    string customerId = check message.customerId.ensureType();

                    // Customer Push
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "customer@foodplatform.na", "PUSH",
                        topic, "Kitchen Update",
                        "The chef has started preparing your delicious meal for order #" + orderId + ".",
                        orderId
                    );

                } else if topic == "orders.ready" {
                    string orderId = check message._id.ensureType();
                    string restaurantId = check message.restaurantId.ensureType();

                    // Restaurant Notification
                    check recordAndSendAlert(
                        restaurantId, "RESTAURANT", "kitchen@restaurant.na", "PUSH",
                        topic, "Order Ready for Pickup",
                        "Order #" + orderId + " is packaged and waiting for the driver.",
                        orderId
                    );

                } else if topic == "orders.cancelled" {
                    string orderId = check message._id.ensureType();
                    string customerId = check message.customerId.ensureType();

                    // Customer SMS and Email
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "+264 81 100 2000", "SMS",
                        topic, "Order Cancelled",
                        "Your order #" + orderId + " was cancelled. If you were charged, a refund is being processed.",
                        orderId
                    );

                } else if topic == "payments.completed" {
                    string orderId = check message.orderId.ensureType();
                    string customerId = check message.customerId.ensureType();
                    float amount = check message.amount.ensureType();

                    // Customer Email receipt
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "customer@foodplatform.na", "EMAIL",
                        topic, "Payment Receipt",
                        "Payment of N$" + amount.toString() + " for order #" + orderId + " was successful.",
                        orderId
                    );

                } else if topic == "payments.failed" {
                    string orderId = check message.orderId.ensureType();
                    string customerId = check message.customerId.ensureType();

                    // Customer SMS Alert
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "+264 81 100 2000", "SMS",
                        topic, "Payment Failed",
                        "Payment processing failed for order #" + orderId + ". Please retry with another payment method.",
                        orderId
                    );

                } else if topic == "delivery.assigned" {
                    string orderId = check message.orderId.ensureType();
                    string customerId = check message.customerId.ensureType();
                    string driverId = check message.driverId.ensureType();
                    string driverName = check message.driverName.ensureType();
                    string driverPhone = check message.driverPhone.ensureType();

                    // Customer Push Alert
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "customer@foodplatform.na", "PUSH",
                        topic, "Driver Dispatched!",
                        "Driver " + driverName + " (" + driverPhone + ") has been assigned to deliver your order.",
                        orderId
                    );

                    // Driver Push Alert
                    check recordAndSendAlert(
                        driverId, "DRIVER", driverPhone, "PUSH",
                        topic, "New Delivery Mission",
                        "You have been assigned order #" + orderId + ". Please navigate to the restaurant for pickup.",
                        orderId
                    );

                } else if topic == "delivery.picked_up" {
                    string orderId = check message.orderId.ensureType();
                    string customerId = check message.customerId.ensureType();

                    // Customer Push Alert
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "customer@foodplatform.na", "PUSH",
                        topic, "Food On The Way!",
                        "Your driver has picked up your food from the restaurant and is heading your way!",
                        orderId
                    );

                } else if topic == "delivery.completed" {
                    string orderId = check message.orderId.ensureType();
                    string customerId = check message.customerId.ensureType();
                    string driverId = check message.driverId.ensureType();

                    // Customer SMS and Push
                    check recordAndSendAlert(
                        customerId, "CUSTOMER", "+264 81 100 2000", "SMS",
                        topic, "Delivered! Bon Appétit!",
                        "Your order #" + orderId + " has arrived. Enjoy your meal!",
                        orderId
                    );

                    // Driver Alert
                    check recordAndSendAlert(
                        driverId, "DRIVER", "+264 81 000 0000", "PUSH",
                        topic, "Delivery Completed",
                        "Great job completing delivery #" + orderId + "! Your earnings have been credited.",
                        orderId
                    );

                } else if topic == "notifications.send" {
                    // Direct generic notification
                    string recipientId = check message.recipientId.ensureType();
                    string recipientType = check message.recipientType.ensureType();
                    string contact = check message.recipientContact.ensureType();
                    string channel = check message.channel.ensureType();
                    string title = check message.title.ensureType();
                    string msg = check message.message.ensureType();
                    string? ordId = ();
                    if message is map<json> && message.hasKey("orderId") && message.get("orderId") is string {
                        ordId = <string>message.get("orderId");
                    }

                    check recordAndSendAlert(recipientId, recipientType, contact, channel, topic, title, msg, ordId);
                }
            } on fail error e {
                log:printError("Error processing event in notification service", 'error = e);
            }
        }
    }
}
