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
final mongodb:Collection paymentsCol = check db->getCollection("payments");

final kafka:Producer producer = check new (kafkaBootstrap);

// Record types
type PaymentStatus "PENDING"|"COMPLETED"|"FAILED"|"REFUNDED";

type Payment record {|
    string id;
    string orderId;
    string customerId;
    decimal amount;
    string method;
    PaymentStatus status;
    string createdAt;
|};

type PaymentRequest record {|
    string orderId;
    string customerId;
    decimal amount;
    string method;
|};

// HTTP Service
service /payments on new http:Listener(8084) {

    // Process payment
    resource function post .(PaymentRequest req) returns json|http:InternalServerError {
        string paymentId = uuid:createType1AsString();
        string now = time:utcToString(time:utcNow());

        // Simulate payment processing (always succeeds for demo)
        Payment payment = {
            id: paymentId,
            orderId: req.orderId,
            customerId: req.customerId,
            amount: req.amount,
            method: req.method,
            status: "COMPLETED",
            createdAt: now
        };

        map<json> doc = {
            "id": payment.id,
            "orderId": payment.orderId,
            "customerId": payment.customerId,
            "amount": payment.amount,
            "method": payment.method,
            "status": payment.status,
            "createdAt": payment.createdAt
        };

        mongodb:Error? insertResult = paymentsCol->insertOne(doc);
        if insertResult is mongodb:Error {
            log:printError("Failed to insert payment", insertResult);
            return <http:InternalServerError>{body: {message: "Failed to process payment"}};
        }

        // Emit Kafka event: payments.completed
        json event = {
            paymentId: paymentId,
            orderId: req.orderId,
            customerId: req.customerId,
            eventType: "PAYMENT_COMPLETED",
            timestamp: now,
            amount: req.amount,
            method: req.method
        };
        kafka:Error? sendResult = producer->send({
            topic: "payments.completed",
            key: req.orderId.toBytes(),
            value: event.toJsonString().toBytes()
        });
        if sendResult is kafka:Error {
            log:printError("Failed to send Kafka event", sendResult);
        }

        log:printInfo("Payment processed: " + paymentId + " for order: " + req.orderId);
        return {paymentId: paymentId, orderId: req.orderId, status: "COMPLETED", amount: req.amount};
    }

    // Get payment by ID
    resource function get [string paymentId]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = paymentsCol->findOne({"id": paymentId});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Payment not found"}};
        }
        return result;
    }

    // Get payment by order ID
    resource function get 'order/[string orderId]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = paymentsCol->findOne({"orderId": orderId});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Payment not found for order"}};
        }
        return result;
    }

    // Get all payments
    resource function get .() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = paymentsCol->find({});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch payments"}};
        }
        json[] payments = [];
        error? e = result.forEach(function(map<json> doc) {
            payments.push(doc);
        });
        if e is error {
            log:printError("Error iterating payments", e);
        }
        return payments;
    }

    // Refund payment
    resource function put [string paymentId]/refund() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = paymentsCol->findOne({"id": paymentId});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Payment not found"}};
        }
        string now = time:utcToString(time:utcNow());
        mongodb:UpdateResult|mongodb:Error updateResult = paymentsCol->updateOne(
            {"id": paymentId},
            {"set": {"status": "REFUNDED"}}
        );
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to refund payment"}};
        }

        // Emit refund event
        json refundEvent = {
            paymentId: paymentId,
            orderId: existing["orderId"],
            eventType: "PAYMENT_REFUNDED",
            timestamp: now
        };
        kafka:Error? sendResult = producer->send({
            topic: "payments.refunded",
            key: paymentId.toBytes(),
            value: refundEvent.toJsonString().toBytes()
        });
        if sendResult is kafka:Error {
            log:printError("Failed to send refund event", sendResult);
        }

        return {paymentId: paymentId, status: "REFUNDED"};
    }
}

// Kafka Consumer: Listen for orders.created to auto-process payment
listener kafka:Listener paymentEventsListener = new (kafkaBootstrap, {
    groupId: "payment-service-group",
    topics: ["orders.created"]
});

service on paymentEventsListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var record in records {
            string value = check string:fromBytes(record.value);
            json payload = check value.fromJsonString();
            string eventType = check payload.eventType.ensureType();

            if eventType == "ORDER_CREATED" {
                string orderId = check payload.orderId.ensureType();
                string customerId = check payload.customerId.ensureType();
                decimal|error total = payload.total.ensureType();
                decimal amount = total is error ? 0d : total;

                string paymentId = uuid:createType1AsString();
                string now = time:utcToString(time:utcNow());

                map<json> doc = {
                    "id": paymentId,
                    "orderId": orderId,
                    "customerId": customerId,
                    "amount": amount,
                    "method": "AUTO",
                    "status": "COMPLETED",
                    "createdAt": now
                };

                mongodb:Error? insertResult = paymentsCol->insertOne(doc);
                if insertResult is mongodb:Error {
                    log:printError("Failed to auto-process payment", insertResult);
                    return;
                }

                // Emit payment completed event
                json event = {
                    paymentId: paymentId,
                    orderId: orderId,
                    customerId: customerId,
                    eventType: "PAYMENT_COMPLETED",
                    timestamp: now,
                    amount: amount,
                    method: "AUTO"
                };
                kafka:Error? sendResult = producer->send({
                    topic: "payments.completed",
                    key: orderId.toBytes(),
                    value: event.toJsonString().toBytes()
                });
                if sendResult is kafka:Error {
                    log:printError("Failed to emit payment event", sendResult);
                }
                log:printInfo("Auto-processed payment for order: " + orderId);
            }
        }
    }
}
