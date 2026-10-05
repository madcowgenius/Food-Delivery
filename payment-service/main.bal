import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerina/time;
import ballerina/random;
import ballerinax/kafka;
import ballerinax/mongodb;

// ──────────────────────────────────────────────
// Configurable Variables
// ──────────────────────────────────────────────
configurable int servicePort = 8084;
configurable string kafkaBootstrapServers = "localhost:9092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string databaseName = "payment_db";

// ──────────────────────────────────────────────
// Record Types
// ──────────────────────────────────────────────

# Payment method enumeration.
enum PaymentMethod {
    CREDIT_CARD,
    DEBIT_CARD,
    MOBILE_MONEY,
    EWALLET,
    CASH_ON_DELIVERY
}

# Payment status enumeration.
enum PaymentStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    REFUNDED
}

# Request payload for manual payment processing.
type PaymentRequest record {|
    string orderId;
    string customerId;
    float amount;
    string method;
|};

// ──────────────────────────────────────────────
// MongoDB and Kafka Initialization
// ──────────────────────────────────────────────
final mongodb:Client mongoClient;
final mongodb:Database db;
const string PAYMENTS_COLLECTION = "payments";
final mongodb:Collection paymentsCollection;

final kafka:Producer kafkaProducer;

const string TOPIC_PAYMENTS_COMPLETED = "payments.completed";
const string TOPIC_PAYMENTS_FAILED = "payments.failed";

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
    paymentsCollection = check db->getCollection(PAYMENTS_COLLECTION);
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
// Helper: Simulate payment processing
// Simulates a payment gateway with ~90% success rate.
// ──────────────────────────────────────────────
function simulatePayment(float amount, string method) returns boolean|error {
    log:printInfo("Processing payment", amount = amount, method = method);
    int randomValue = check random:createIntInRange(1, 100);
    return randomValue <= 90;
}

// ──────────────────────────────────────────────
// Helper: Publish payment event to Kafka
// ──────────────────────────────────────────────
function publishPaymentEvent(string topic, json payload) returns error? {
    byte[] serializedMsg = payload.toJsonString().toBytes();
    check kafkaProducer->send({
        topic: topic,
        value: serializedMsg
    });
    log:printInfo("Published payment event", topic = topic);
}

// ──────────────────────────────────────────────
// HTTP REST API Service
// ──────────────────────────────────────────────
service /api/v1 on new http:Listener(servicePort) {

    # Health check endpoint.
    resource function get health() returns json {
        return {"status": "UP", "service": "payment-service"};
    }

    # Process a payment manually.
    # + payload - Payment details
    # + return - Payment result
    resource function post payments(@http:Payload PaymentRequest payload) returns http:Created|http:InternalServerError {
        do {
            string paymentId = uuid:createType1AsString();
            string transactionId = uuid:createType1AsString();
            string currentTime = time:utcToString(time:utcNow());

            boolean success = check simulatePayment(payload.amount, payload.method);
            string status = success ? "COMPLETED" : "FAILED";

            map<json> paymentDoc = {
                "_id": paymentId,
                "orderId": payload.orderId,
                "customerId": payload.customerId,
                "amount": payload.amount,
                "method": payload.method,
                "status": status,
                "transactionId": transactionId,
                "createdAt": currentTime,
                "updatedAt": currentTime
            };

            check paymentsCollection->insertOne(paymentDoc);

            if success {
                check publishPaymentEvent(TOPIC_PAYMENTS_COMPLETED, paymentDoc);
                log:printInfo("Payment completed", paymentId = paymentId, orderId = payload.orderId);
            } else {
                check publishPaymentEvent(TOPIC_PAYMENTS_FAILED, paymentDoc);
                log:printInfo("Payment failed", paymentId = paymentId, orderId = payload.orderId);
            }

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Payment " + status.toLowerAscii(),
                    "data": paymentDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to process payment", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve a payment by ID.
    # + paymentId - The unique payment identifier
    # + return - Payment details
    resource function get payments/[string paymentId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": paymentId};
            map<json>[] results = check queryCollection(paymentsCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Payment not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve payment", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve payment by order ID.
    # + orderId - The unique order identifier
    # + return - Payment details for the order
    resource function get payments/'order/[string orderId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"orderId": orderId};
            map<json>[] results = check queryCollection(paymentsCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "No payment found for this order"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve payment by order", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all payments.
    # + return - List of all payments
    resource function get payments() returns json|http:InternalServerError {
        do {
            map<json>[] payments = check queryCollection(paymentsCollection, {});

            return {
                "status": "success",
                "count": payments.length(),
                "data": payments
            };
        } on fail error e {
            log:printError("Failed to retrieve payments", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Process a refund for a payment.
    # + paymentId - The unique payment identifier
    # + return - Refund result
    resource function post payments/[string paymentId]/refund() returns json|http:BadRequest|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": paymentId};
            map<json>[] results = check queryCollection(paymentsCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Payment not found"}
                };
                return notFound;
            }

            string currentPaymentStatus = check results[0]["status"].ensureType();
            if currentPaymentStatus != "COMPLETED" {
                http:BadRequest badRequest = {
                    body: {"status": "error", "message": "Can only refund completed payments"}
                };
                return badRequest;
            }

            string currentTime = time:utcToString(time:utcNow());
            mongodb:Update update = {
                set: {
                    "status": "REFUNDED",
                    "refundedAt": currentTime,
                    "updatedAt": currentTime
                }
            };

            _ = check paymentsCollection->updateOne(filter, update);

            log:printInfo("Payment refunded", paymentId = paymentId);

            return {
                "status": "success",
                "message": "Payment refunded successfully"
            };
        } on fail error e {
            log:printError("Failed to refund payment", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }
}

// ──────────────────────────────────────────────
// Kafka Consumer: Listen for new order events
// Automatically processes payment when an order is created
// ──────────────────────────────────────────────
listener kafka:Listener paymentKafkaListener = new (kafkaBootstrapServers, {
    groupId: "payment-service-group",
    topics: ["orders.created"],
    pollingInterval: 1
});

service on paymentKafkaListener {

    # Handles incoming order events to automatically process payment.
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            do {
                string messageStr = check string:fromBytes(rec.value);
                json message = check messageStr.fromJsonString();

                log:printInfo("Received order.created event for payment processing");

                string orderId = check message._id.ensureType();
                string customerId = check message.customerId.ensureType();
                float totalAmount = check message.totalAmount.ensureType();

                string paymentId = uuid:createType1AsString();
                string transactionId = uuid:createType1AsString();
                string currentTime = time:utcToString(time:utcNow());

                boolean success = check simulatePayment(totalAmount, "CREDIT_CARD");
                string status = success ? "COMPLETED" : "FAILED";

                map<json> paymentDoc = {
                    "_id": paymentId,
                    "orderId": orderId,
                    "customerId": customerId,
                    "amount": totalAmount,
                    "method": "CREDIT_CARD",
                    "status": status,
                    "transactionId": transactionId,
                    "createdAt": currentTime,
                    "updatedAt": currentTime
                };

                check paymentsCollection->insertOne(paymentDoc);

                if success {
                    check publishPaymentEvent(TOPIC_PAYMENTS_COMPLETED, paymentDoc);
                    log:printInfo("Auto-payment completed for order", orderId = orderId);
                } else {
                    check publishPaymentEvent(TOPIC_PAYMENTS_FAILED, paymentDoc);
                    log:printInfo("Auto-payment failed for order", orderId = orderId);
                }
            } on fail error e {
                log:printError("Error processing order for payment", 'error = e);
            }
        }
    }
}
