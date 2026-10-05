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
final mongodb:Collection deliveriesCol = check db->getCollection("deliveries");
final mongodb:Collection driversCol = check db->getCollection("drivers");

final kafka:Producer producer = check new (kafkaBootstrap);

// Record types
type DeliveryStatus "ASSIGNED"|"PICKED_UP"|"IN_TRANSIT"|"DELIVERED";

type Driver record {|
    string id;
    string name;
    string phone;
    string vehicleType;
    boolean available;
    decimal latitude;
    decimal longitude;
|};

type DriverRequest record {|
    string name;
    string phone;
    string vehicleType;
|};

type Delivery record {|
    string id;
    string orderId;
    string driverId;
    string restaurantAddress;
    string customerAddress;
    DeliveryStatus status;
    string createdAt;
    string updatedAt;
|};

type DeliveryRequest record {|
    string orderId;
    string restaurantAddress;
    string customerAddress;
|};

// HTTP Service
service /deliveries on new http:Listener(8085) {

    // ====== Driver Management ======

    // Register a new driver
    resource function post drivers(DriverRequest req) returns json|http:InternalServerError {
        string id = uuid:createType1AsString();
        Driver driver = {
            id: id,
            name: req.name,
            phone: req.phone,
            vehicleType: req.vehicleType,
            available: true,
            latitude: 0d,
            longitude: 0d
        };
        map<json> doc = {
            "id": driver.id,
            "name": driver.name,
            "phone": driver.phone,
            "vehicleType": driver.vehicleType,
            "available": driver.available,
            "latitude": driver.latitude,
            "longitude": driver.longitude
        };
        mongodb:Error? insertResult = driversCol->insertOne(doc);
        if insertResult is mongodb:Error {
            log:printError("Failed to register driver", insertResult);
            return <http:InternalServerError>{body: {message: "Failed to register driver"}};
        }
        log:printInfo("Driver registered: " + id);
        return driver;
    }

    // Get all drivers
    resource function get drivers() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = driversCol->find({});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch drivers"}};
        }
        json[] drivers = [];
        error? e = result.forEach(function(map<json> doc) {
            drivers.push(doc);
        });
        if e is error {
            log:printError("Error iterating drivers", e);
        }
        return drivers;
    }

    // Get available drivers
    resource function get drivers/available() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = driversCol->find({"available": true});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch available drivers"}};
        }
        json[] drivers = [];
        error? e = result.forEach(function(map<json> doc) {
            drivers.push(doc);
        });
        if e is error {
            log:printError("Error iterating drivers", e);
        }
        return drivers;
    }

    // Update driver location
    resource function put drivers/[string driverId]/location(json locationReq) returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = driversCol->findOne({"id": driverId});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Driver not found"}};
        }
        decimal|error lat = locationReq.latitude.ensureType();
        decimal|error lng = locationReq.longitude.ensureType();
        if lat is error || lng is error {
            return <http:InternalServerError>{body: {message: "Invalid coordinates"}};
        }
        mongodb:UpdateResult|mongodb:Error updateResult = driversCol->updateOne(
            {"id": driverId},
            {"set": {"latitude": lat, "longitude": lng}}
        );
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to update location"}};
        }
        return {driverId: driverId, latitude: lat, longitude: lng};
    }

    // ====== Delivery Management ======

    // Create delivery assignment
    resource function post .(DeliveryRequest req) returns json|http:InternalServerError {
        // Find an available driver
        stream<map<json>, error?>|mongodb:Error availableDrivers = driversCol->find({"available": true});
        if availableDrivers is mongodb:Error {
            log:printError("Failed to find available drivers", availableDrivers);
            return <http:InternalServerError>{body: {message: "No drivers available"}};
        }

        string? selectedDriverId = ();
        error? iterErr = availableDrivers.forEach(function(map<json> doc) {
            if selectedDriverId is () {
                string|error dId = doc["id"].ensureType();
                if dId is string {
                    selectedDriverId = dId;
                }
            }
        });
        if iterErr is error {
            log:printError("Error finding drivers", iterErr);
        }

        if selectedDriverId is () {
            return <http:InternalServerError>{body: {message: "No drivers available"}};
        }

        string deliveryId = uuid:createType1AsString();
        string now = time:utcToString(time:utcNow());

        Delivery delivery = {
            id: deliveryId,
            orderId: req.orderId,
            driverId: <string>selectedDriverId,
            restaurantAddress: req.restaurantAddress,
            customerAddress: req.customerAddress,
            status: "ASSIGNED",
            createdAt: now,
            updatedAt: now
        };

        map<json> doc = {
            "id": delivery.id,
            "orderId": delivery.orderId,
            "driverId": delivery.driverId,
            "restaurantAddress": delivery.restaurantAddress,
            "customerAddress": delivery.customerAddress,
            "status": delivery.status,
            "createdAt": delivery.createdAt,
            "updatedAt": delivery.updatedAt
        };

        mongodb:Error? insertResult = deliveriesCol->insertOne(doc);
        if insertResult is mongodb:Error {
            log:printError("Failed to create delivery", insertResult);
            return <http:InternalServerError>{body: {message: "Failed to create delivery"}};
        }

        // Mark driver as unavailable
        mongodb:UpdateResult|mongodb:Error driverUpdate = driversCol->updateOne(
            {"id": <string>selectedDriverId},
            {"set": {"available": false}}
        );
        if driverUpdate is mongodb:Error {
            log:printError("Failed to update driver availability", driverUpdate);
        }

        // Emit Kafka event: delivery.assigned
        json event = {
            deliveryId: deliveryId,
            orderId: req.orderId,
            driverId: selectedDriverId,
            eventType: "DELIVERY_ASSIGNED",
            timestamp: now
        };
        kafka:Error? sendResult = producer->send({
            topic: "delivery.assigned",
            key: req.orderId.toBytes(),
            value: event.toJsonString().toBytes()
        });
        if sendResult is kafka:Error {
            log:printError("Failed to send Kafka event", sendResult);
        }

        log:printInfo("Delivery assigned: " + deliveryId + " to driver: " + <string>selectedDriverId);
        return {deliveryId: deliveryId, orderId: req.orderId, driverId: selectedDriverId, status: "ASSIGNED"};
    }

    // Get delivery by ID
    resource function get [string deliveryId]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = deliveriesCol->findOne({"id": deliveryId});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Delivery not found"}};
        }
        return result;
    }

    // Get delivery by order ID
    resource function get 'order/[string orderId]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = deliveriesCol->findOne({"orderId": orderId});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Delivery not found for order"}};
        }
        return result;
    }

    // Update delivery status
    resource function put [string deliveryId]/status(json statusReq) returns json|http:NotFound|http:BadRequest|http:InternalServerError {
        map<json>|mongodb:Error? existing = deliveriesCol->findOne({"id": deliveryId});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Delivery not found"}};
        }

        string|error newStatusStr = statusReq.status.ensureType();
        if newStatusStr is error {
            return <http:BadRequest>{body: {message: "Invalid status"}};
        }

        string now = time:utcToString(time:utcNow());
        mongodb:UpdateResult|mongodb:Error updateResult = deliveriesCol->updateOne(
            {"id": deliveryId},
            {"set": {"status": newStatusStr, "updatedAt": now}}
        );
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to update delivery status"}};
        }

        string|error orderId = existing["orderId"].ensureType();
        string orderIdStr = orderId is string ? orderId : "";

        // If delivered, emit delivery.completed and free up driver
        if newStatusStr == "DELIVERED" {
            json completedEvent = {
                deliveryId: deliveryId,
                orderId: orderIdStr,
                eventType: "DELIVERY_COMPLETED",
                timestamp: now
            };
            kafka:Error? sendResult = producer->send({
                topic: "delivery.completed",
                key: orderIdStr.toBytes(),
                value: completedEvent.toJsonString().toBytes()
            });
            if sendResult is kafka:Error {
                log:printError("Failed to send delivery completed event", sendResult);
            }

            // Mark driver as available again
            string|error driverId = existing["driverId"].ensureType();
            if driverId is string {
                mongodb:UpdateResult|mongodb:Error driverUpdate = driversCol->updateOne(
                    {"id": driverId},
                    {"set": {"available": true}}
                );
                if driverUpdate is mongodb:Error {
                    log:printError("Failed to free driver", driverUpdate);
                }
            }
            log:printInfo("Delivery completed: " + deliveryId);
        }

        return {deliveryId: deliveryId, status: newStatusStr};
    }

    // Get all deliveries
    resource function get .() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = deliveriesCol->find({});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch deliveries"}};
        }
        json[] deliveries = [];
        error? e = result.forEach(function(map<json> doc) {
            deliveries.push(doc);
        });
        if e is error {
            log:printError("Error iterating deliveries", e);
        }
        return deliveries;
    }
}

// Kafka Consumer: Listen for payments.completed to auto-assign delivery
listener kafka:Listener deliveryEventsListener = new (kafkaBootstrap, {
    groupId: "delivery-service-group",
    topics: ["payments.completed"]
});

service on deliveryEventsListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var record in records {
            string value = check string:fromBytes(record.value);
            json payload = check value.fromJsonString();
            string eventType = check payload.eventType.ensureType();

            if eventType == "PAYMENT_COMPLETED" {
                string orderId = check payload.orderId.ensureType();

                // Find an available driver
                stream<map<json>, error?>|mongodb:Error availableDrivers = driversCol->find({"available": true});
                if availableDrivers is mongodb:Error {
                    log:printError("No available drivers for auto-assignment");
                    return;
                }

                string? selectedDriverId = ();
                error? iterErr = availableDrivers.forEach(function(map<json> doc) {
                    if selectedDriverId is () {
                        string|error dId = doc["id"].ensureType();
                        if dId is string {
                            selectedDriverId = dId;
                        }
                    }
                });
                if iterErr is error {
                    log:printError("Error iterating drivers", iterErr);
                }

                if selectedDriverId is () {
                    log:printError("No available drivers for order: " + orderId);
                    return;
                }

                string deliveryId = uuid:createType1AsString();
                string now = time:utcToString(time:utcNow());

                map<json> doc = {
                    "id": deliveryId,
                    "orderId": orderId,
                    "driverId": selectedDriverId,
                    "restaurantAddress": "Auto-assigned",
                    "customerAddress": "Auto-assigned",
                    "status": "ASSIGNED",
                    "createdAt": now,
                    "updatedAt": now
                };

                mongodb:Error? insertResult = deliveriesCol->insertOne(doc);
                if insertResult is mongodb:Error {
                    log:printError("Failed to auto-assign delivery", insertResult);
                    return;
                }

                // Mark driver unavailable
                mongodb:UpdateResult|mongodb:Error driverUpdate = driversCol->updateOne(
                    {"id": <string>selectedDriverId},
                    {"set": {"available": false}}
                );
                if driverUpdate is mongodb:Error {
                    log:printError("Failed to update driver availability", driverUpdate);
                }

                // Emit delivery.assigned event
                json event = {
                    deliveryId: deliveryId,
                    orderId: orderId,
                    driverId: selectedDriverId,
                    eventType: "DELIVERY_ASSIGNED",
                    timestamp: now
                };
                kafka:Error? sendResult = producer->send({
                    topic: "delivery.assigned",
                    key: orderId.toBytes(),
                    value: event.toJsonString().toBytes()
                });
                if sendResult is kafka:Error {
                    log:printError("Failed to emit delivery assigned event", sendResult);
                }
                log:printInfo("Auto-assigned delivery " + deliveryId + " for order: " + orderId);
            }
        }
    }
}
