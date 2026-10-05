import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

// ──────────────────────────────────────────────
// Configurable Variables
// ──────────────────────────────────────────────
configurable int servicePort = 8085;
configurable string kafkaBootstrapServers = "localhost:9092";
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string databaseName = "delivery_db";

// ──────────────────────────────────────────────
// Record Types
// ──────────────────────────────────────────────

# Driver details record.
type Driver record {|
    string _id;
    string name;
    string phone;
    string vehicleType;
    string licensePlate;
    string status; // AVAILABLE, BUSY, OFFLINE
    float currentLatitude;
    float currentLongitude;
    float rating;
    int totalDeliveries;
    string createdAt;
    string updatedAt;
|};

# Payload to register a new driver.
type DriverRequest record {|
    string name;
    string phone;
    string vehicleType;
    string licensePlate;
    float latitude = -22.5609;
    float longitude = 17.0658;
|};

# Payload to update driver status.
type DriverStatusRequest record {|
    string status;
|};

# Payload to update driver location coordinates.
type DriverLocationRequest record {|
    float latitude;
    float longitude;
|};

# Represents a delivery assignment.
type Delivery record {|
    string _id;
    string orderId;
    string driverId;
    string driverName;
    string driverPhone;
    string restaurantId;
    string customerId;
    string status; // ASSIGNED, PICKED_UP, DELIVERED, CANCELLED
    string deliveryAddress;
    float pickupLatitude;
    float pickupLongitude;
    float destinationLatitude;
    float destinationLongitude;
    float currentLatitude;
    float currentLongitude;
    float distanceKm;
    int estimatedMinutes;
    int progressPercent; // 0 to 100
    string assignedAt;
    string? pickedUpAt;
    string? deliveredAt;
    string updatedAt;
|};

# Request payload to create a delivery manually.
type DeliveryRequest record {|
    string orderId;
    string restaurantId;
    string customerId;
    string deliveryAddress;
    string? driverId = ();
    float pickupLatitude = -22.5600;
    float pickupLongitude = 17.0650;
    float destinationLatitude = -22.5750;
    float destinationLongitude = 17.0850;
|};

# Request payload to update delivery status.
type DeliveryStatusUpdateRequest record {|
    string status;
|};

// ──────────────────────────────────────────────
// MongoDB and Kafka Initialization
// ──────────────────────────────────────────────
final mongodb:Client mongoClient;
final mongodb:Database db;
const string DRIVERS_COLLECTION = "drivers";
const string DELIVERIES_COLLECTION = "deliveries";
final mongodb:Collection driversCollection;
final mongodb:Collection deliveriesCollection;

final kafka:Producer kafkaProducer;

const string TOPIC_DELIVERY_ASSIGNED = "delivery.assigned";
const string TOPIC_DELIVERY_PICKED_UP = "delivery.picked_up";
const string TOPIC_DELIVERY_COMPLETED = "delivery.completed";

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
    driversCollection = check db->getCollection(DRIVERS_COLLECTION);
    deliveriesCollection = check db->getCollection(DELIVERIES_COLLECTION);
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
// Helper: Haversine / Distance Approximation in km
// ──────────────────────────────────────────────
function calculateDistance(float lat1, float lon1, float lat2, float lon2) returns float {
    float dLat = (lat2 - lat1) * 111.0;
    float dLon = (lon2 - lon1) * 111.0 * 0.9;
    float sumSq = dLat * dLat + dLon * dLon;
    if sumSq <= 0.0 {
        return 0.0;
    }
    float x = sumSq;
    float y = 1.0;
    float e = 0.001;
    while (x - y) > e || (y - x) > e {
        x = (x + y) / 2.0;
        y = sumSq / x;
    }
    return x;
}

// ──────────────────────────────────────────────
// Helper: Calculate ETA in minutes based on distance (30 km/h avg speed)
// ──────────────────────────────────────────────
function calculateEta(float distanceKm) returns int {
    float hours = distanceKm / 30.0;
    int minutes = <int>(hours * 60.0) + 5; // +5 mins buffer for pickup
    if minutes < 10 {
        return 10;
    }
    return minutes;
}

// ──────────────────────────────────────────────
// Helper: Publish Delivery Event to Kafka
// ──────────────────────────────────────────────
function publishDeliveryEvent(string topic, json payload) returns error? {
    byte[] serializedMsg = payload.toJsonString().toBytes();
    check kafkaProducer->send({
        topic: topic,
        value: serializedMsg
    });
    log:printInfo("Published delivery event", topic = topic);
}

// ──────────────────────────────────────────────
// HTTP REST API Service
// ──────────────────────────────────────────────
service /api/v1 on new http:Listener(servicePort) {

    # Health check endpoint.
    resource function get health() returns json {
        return {"status": "UP", "service": "delivery-service"};
    }

    // ─── Driver Management Endpoints ──────────

    # Register a new driver.
    # + payload - Driver registration info
    # + return - Created driver or error
    resource function post drivers(@http:Payload DriverRequest payload) returns http:Created|http:InternalServerError {
        do {
            string driverId = uuid:createType1AsString();
            string currentTime = time:utcToString(time:utcNow());

            map<json> driverDoc = {
                "_id": driverId,
                "name": payload.name,
                "phone": payload.phone,
                "vehicleType": payload.vehicleType,
                "licensePlate": payload.licensePlate,
                "status": "AVAILABLE",
                "currentLatitude": payload.latitude,
                "currentLongitude": payload.longitude,
                "rating": 5.0,
                "totalDeliveries": 0,
                "createdAt": currentTime,
                "updatedAt": currentTime
            };

            check driversCollection->insertOne(driverDoc);

            log:printInfo("Driver registered", driverId = driverId, name = payload.name);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Driver registered successfully",
                    "data": driverDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to register driver", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all drivers, with optional status filter.
    # + status - Optional status filter (AVAILABLE, BUSY, OFFLINE)
    # + return - List of drivers
    resource function get drivers(string? status) returns json|http:InternalServerError {
        do {
            map<json> filter = {};
            if status is string {
                filter["status"] = status;
            }

            map<json>[] drivers = check queryCollection(driversCollection, filter);

            return {
                "status": "success",
                "count": drivers.length(),
                "data": drivers
            };
        } on fail error e {
            log:printError("Failed to retrieve drivers", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve single driver by ID.
    # + driverId - The unique driver identifier
    # + return - Driver record or not found
    resource function get drivers/[string driverId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": driverId};
            map<json>[] results = check queryCollection(driversCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Driver not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve driver", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Update driver status.
    # + driverId - The unique driver identifier
    # + payload - New driver status
    # + return - Updated driver or error
    resource function put drivers/[string driverId]/status(@http:Payload DriverStatusRequest payload) returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": driverId};
            string currentTime = time:utcToString(time:utcNow());

            mongodb:Update update = {
                set: {
                    "status": payload.status,
                    "updatedAt": currentTime
                }
            };

            mongodb:UpdateResult res = check driversCollection->updateOne(filter, update);
            if res.matchedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Driver not found"}
                };
                return notFound;
            }

            map<json>[] updated = check queryCollection(driversCollection, filter);
            return {
                "status": "success",
                "message": "Driver status updated to " + payload.status,
                "data": updated[0]
            };
        } on fail error e {
            log:printError("Failed to update driver status", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Update driver location coordinates.
    # + driverId - The unique driver identifier
    # + payload - New GPS coordinates
    # + return - Updated location or error
    resource function put drivers/[string driverId]/location(@http:Payload DriverLocationRequest payload) returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": driverId};
            string currentTime = time:utcToString(time:utcNow());

            mongodb:Update update = {
                set: {
                    "currentLatitude": payload.latitude,
                    "currentLongitude": payload.longitude,
                    "updatedAt": currentTime
                }
            };

            mongodb:UpdateResult res = check driversCollection->updateOne(filter, update);
            if res.matchedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Driver not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "message": "Driver location updated",
                "latitude": payload.latitude,
                "longitude": payload.longitude
            };
        } on fail error e {
            log:printError("Failed to update driver location", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    // ─── Delivery Lifecycle Endpoints ──────────

    # Create and assign a delivery.
    # Finds available driver or uses specified driver, computes distance & ETA, and emits `delivery.assigned`.
    # + payload - Delivery assignment details
    # + return - Created delivery record
    resource function post deliveries(@http:Payload DeliveryRequest payload) returns http:Created|http:BadRequest|http:InternalServerError {
        do {
            string assignedDriverId = "";
            string assignedDriverName = "Express Driver";
            string assignedDriverPhone = "+264 81 000 0000";
            float initialLat = payload.pickupLatitude;
            float initialLon = payload.pickupLongitude;

            if payload.driverId is string && (<string>payload.driverId).trim().length() > 0 {
                assignedDriverId = <string>payload.driverId;
                map<json> dFilter = {"_id": assignedDriverId};
                map<json>[] dRes = check queryCollection(driversCollection, dFilter);
                if dRes.length() > 0 {
                    assignedDriverName = check dRes[0]["name"].ensureType();
                    assignedDriverPhone = check dRes[0]["phone"].ensureType();
                    if dRes[0]["currentLatitude"] is float {
                        initialLat = <float>dRes[0]["currentLatitude"];
                        initialLon = <float>dRes[0]["currentLongitude"];
                    }
                }
            } else {
                // Auto-assign nearest AVAILABLE driver
                map<json> availFilter = {"status": "AVAILABLE"};
                map<json>[] availDrivers = check queryCollection(driversCollection, availFilter);
                if availDrivers.length() > 0 {
                    // Pick the closest driver
                    int bestIdx = 0;
                    float minDistance = 999999.0;
                    foreach int i in 0 ..< availDrivers.length() {
                        float dLat = payload.pickupLatitude;
                        float dLon = payload.pickupLongitude;
                        if availDrivers[i]["currentLatitude"] is float {
                            dLat = <float>availDrivers[i]["currentLatitude"];
                            dLon = <float>availDrivers[i]["currentLongitude"];
                        }
                        float dist = calculateDistance(payload.pickupLatitude, payload.pickupLongitude, dLat, dLon);
                        if dist < minDistance {
                            minDistance = dist;
                            bestIdx = i;
                        }
                    }
                    map<json> chosenDriver = availDrivers[bestIdx];
                    assignedDriverId = check chosenDriver["_id"].ensureType();
                    assignedDriverName = check chosenDriver["name"].ensureType();
                    assignedDriverPhone = check chosenDriver["phone"].ensureType();
                    if chosenDriver["currentLatitude"] is float {
                        initialLat = <float>chosenDriver["currentLatitude"];
                        initialLon = <float>chosenDriver["currentLongitude"];
                    }
                } else {
                    http:BadRequest badRequest = {
                        body: {
                            "status": "error",
                            "message": "No available drivers found to assign this delivery"
                        }
                    };
                    return badRequest;
                }
            }

            string deliveryId = uuid:createType1AsString();
            string currentTime = time:utcToString(time:utcNow());
            float distanceKm = calculateDistance(payload.pickupLatitude, payload.pickupLongitude, payload.destinationLatitude, payload.destinationLongitude);
            int estimatedMins = calculateEta(distanceKm);

            map<json> deliveryDoc = {
                "_id": deliveryId,
                "orderId": payload.orderId,
                "driverId": assignedDriverId,
                "driverName": assignedDriverName,
                "driverPhone": assignedDriverPhone,
                "restaurantId": payload.restaurantId,
                "customerId": payload.customerId,
                "status": "ASSIGNED",
                "deliveryAddress": payload.deliveryAddress,
                "pickupLatitude": payload.pickupLatitude,
                "pickupLongitude": payload.pickupLongitude,
                "destinationLatitude": payload.destinationLatitude,
                "destinationLongitude": payload.destinationLongitude,
                "currentLatitude": initialLat,
                "currentLongitude": initialLon,
                "distanceKm": distanceKm,
                "estimatedMinutes": estimatedMins,
                "progressPercent": 10,
                "assignedAt": currentTime,
                "pickedUpAt": (),
                "deliveredAt": (),
                "updatedAt": currentTime
            };

            check deliveriesCollection->insertOne(deliveryDoc);

            // Mark driver as BUSY
            map<json> drvFilter = {"_id": assignedDriverId};
            mongodb:Update drvUpdate = {set: {"status": "BUSY", "updatedAt": currentTime}};
            _ = check driversCollection->updateOne(drvFilter, drvUpdate);

            // Publish delivery.assigned event to Kafka
            check publishDeliveryEvent(TOPIC_DELIVERY_ASSIGNED, deliveryDoc);

            log:printInfo("Delivery assigned", deliveryId = deliveryId, orderId = payload.orderId, driverId = assignedDriverId);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Delivery assigned successfully",
                    "data": deliveryDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to assign delivery", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all deliveries.
    # + status - Optional status filter
    # + return - List of deliveries
    resource function get deliveries(string? status) returns json|http:InternalServerError {
        do {
            map<json> filter = {};
            if status is string {
                filter["status"] = status;
            }

            map<json>[] deliveries = check queryCollection(deliveriesCollection, filter);

            return {
                "status": "success",
                "count": deliveries.length(),
                "data": deliveries
            };
        } on fail error e {
            log:printError("Failed to retrieve deliveries", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve delivery by ID.
    # + deliveryId - Unique delivery ID
    # + return - Delivery details
    resource function get deliveries/[string deliveryId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": deliveryId};
            map<json>[] results = check queryCollection(deliveriesCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Delivery not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve delivery", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve delivery associated with an order ID.
    # + orderId - Unique order ID
    # + return - Delivery details
    resource function get deliveries/'order/[string orderId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"orderId": orderId};
            map<json>[] results = check queryCollection(deliveriesCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "No delivery found for this order"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve delivery for order", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Update delivery status (e.g. PICKED_UP, DELIVERED).
    # Emits Kafka events `delivery.picked_up` or `delivery.completed`.
    # + deliveryId - Unique delivery ID
    # + payload - New delivery status
    # + return - Updated delivery details
    resource function put deliveries/[string deliveryId]/status(@http:Payload DeliveryStatusUpdateRequest payload) returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": deliveryId};
            map<json>[] results = check queryCollection(deliveriesCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Delivery not found"}
                };
                return notFound;
            }

            string newStatus = payload.status;
            string currentTime = time:utcToString(time:utcNow());
            string driverId = check results[0]["driverId"].ensureType();

            map<json> updateFields = {
                "status": newStatus,
                "updatedAt": currentTime
            };

            if newStatus == "PICKED_UP" {
                updateFields["pickedUpAt"] = currentTime;
                updateFields["progressPercent"] = 40;
            } else if newStatus == "DELIVERED" {
                updateFields["deliveredAt"] = currentTime;
                updateFields["progressPercent"] = 100;
            }

            mongodb:Update update = {set: updateFields};
            _ = check deliveriesCollection->updateOne(filter, update);

            map<json>[] updatedRes = check queryCollection(deliveriesCollection, filter);
            json updatedDelivery = updatedRes[0];

            if newStatus == "PICKED_UP" {
                check publishDeliveryEvent(TOPIC_DELIVERY_PICKED_UP, updatedDelivery);
                log:printInfo("Delivery picked up", deliveryId = deliveryId);
            } else if newStatus == "DELIVERED" {
                check publishDeliveryEvent(TOPIC_DELIVERY_COMPLETED, updatedDelivery);

                // Set driver back to AVAILABLE and increment completed count
                map<json> dFilter = {"_id": driverId};
                map<json>[] dRes = check queryCollection(driversCollection, dFilter);
                int count = 1;
                if dRes.length() > 0 && dRes[0]["totalDeliveries"] is int {
                    count = (<int>dRes[0]["totalDeliveries"]) + 1;
                }
                mongodb:Update dUpdate = {
                    set: {
                        "status": "AVAILABLE",
                        "totalDeliveries": count,
                        "updatedAt": currentTime
                    }
                };
                _ = check driversCollection->updateOne(dFilter, dUpdate);

                log:printInfo("Delivery completed", deliveryId = deliveryId, driverId = driverId);
            }

            return {
                "status": "success",
                "message": "Delivery status updated to " + newStatus,
                "data": updatedDelivery
            };
        } on fail error e {
            log:printError("Failed to update delivery status", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    // ─── Bonus Extensions: Simulation & Route Optimization ───────

    # Simulates a driver movement step towards the customer destination (Bonus: Driver Location Simulation).
    # Advances progress by ~25% and updates interpolated coordinates.
    # + deliveryId - The unique delivery identifier
    # + return - Updated position and simulation progress
    resource function post deliveries/[string deliveryId]/simulate\-step() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": deliveryId};
            map<json>[] results = check queryCollection(deliveriesCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Delivery not found"}
                };
                return notFound;
            }

            map<json> del = results[0];
            int currentProg = 10;
            if del["progressPercent"] is int {
                currentProg = <int>del["progressPercent"];
            }

            int nextProg = currentProg + 25;
            if nextProg > 100 {
                nextProg = 100;
            }

            float pLat = <float>del["pickupLatitude"];
            float pLon = <float>del["pickupLongitude"];
            float dLat = <float>del["destinationLatitude"];
            float dLon = <float>del["destinationLongitude"];

            // Interpolate coordinate along fraction
            float fraction = <float>nextProg / 100.0;
            float newLat = pLat + (dLat - pLat) * fraction;
            float newLon = pLon + (dLon - pLon) * fraction;

            string currentTime = time:utcToString(time:utcNow());
            string newStatus = "IN_TRANSIT";
            if nextProg >= 100 {
                newStatus = "DELIVERED";
            } else if nextProg >= 25 {
                newStatus = "PICKED_UP";
            }

            map<json> updateFields = {
                "currentLatitude": newLat,
                "currentLongitude": newLon,
                "progressPercent": nextProg,
                "status": newStatus,
                "updatedAt": currentTime
            };

            if nextProg >= 100 {
                updateFields["deliveredAt"] = currentTime;
            } else if nextProg >= 25 && del["pickedUpAt"] is () {
                updateFields["pickedUpAt"] = currentTime;
            }

            mongodb:Update update = {set: updateFields};
            _ = check deliveriesCollection->updateOne(filter, update);

            // Also update driver's own location in drivers collection
            string driverId = check del["driverId"].ensureType();
            map<json> drvFilter = {"_id": driverId};
            map<json> drvFields = {
                "currentLatitude": newLat,
                "currentLongitude": newLon,
                "updatedAt": currentTime
            };
            if nextProg >= 100 {
                drvFields["status"] = "AVAILABLE";
            }
            mongodb:Update drvUpdate = {set: drvFields};
            _ = check driversCollection->updateOne(drvFilter, drvUpdate);

            map<json>[] updatedResults = check queryCollection(deliveriesCollection, filter);
            json updatedDelivery = updatedResults[0];

            if nextProg >= 100 {
                check publishDeliveryEvent(TOPIC_DELIVERY_COMPLETED, updatedDelivery);
            } else if nextProg == 25 {
                check publishDeliveryEvent(TOPIC_DELIVERY_PICKED_UP, updatedDelivery);
            }

            return {
                "status": "success",
                "message": "Driver moved forward along route",
                "progressPercent": nextProg,
                "currentLatitude": newLat,
                "currentLongitude": newLon,
                "deliveryStatus": newStatus
            };
        } on fail error e {
            log:printError("Failed to simulate delivery step", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Route optimization and waypoints inspection (Bonus: Route Optimization).
    # Returns calculated route geometry, distance, and estimated time.
    # + deliveryId - The unique delivery identifier
    # + return - Route details and waypoints
    resource function get deliveries/[string deliveryId]/route() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": deliveryId};
            map<json>[] results = check queryCollection(deliveriesCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Delivery not found"}
                };
                return notFound;
            }

            map<json> del = results[0];
            float pLat = <float>del["pickupLatitude"];
            float pLon = <float>del["pickupLongitude"];
            float dLat = <float>del["destinationLatitude"];
            float dLon = <float>del["destinationLongitude"];
            float curLat = <float>del["currentLatitude"];
            float curLon = <float>del["currentLongitude"];
            float distanceKm = <float>del["distanceKm"];
            int estMins = <int>del["estimatedMinutes"];

            // Generate 5 waypoint coordinates along the optimized path
            json[] waypoints = [];
            foreach int step in 0 ... 4 {
                float f = <float>step / 4.0;
                waypoints.push({
                    "step": step,
                    "latitude": pLat + (dLat - pLat) * f,
                    "longitude": pLon + (dLon - pLon) * f
                });
            }

            return {
                "status": "success",
                "deliveryId": deliveryId,
                "distanceKm": distanceKm,
                "estimatedMinutes": estMins,
                "currentPosition": {
                    "latitude": curLat,
                    "longitude": curLon
                },
                "destination": {
                    "latitude": dLat,
                    "longitude": dLon
                },
                "waypoints": waypoints
            };
        } on fail error e {
            log:printError("Failed to calculate route", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Dynamic Surge Pricing Factor endpoint (Bonus: Surge Pricing).
    # Calculates dynamic price multiplier based on available vs busy drivers.
    # + return - Surge multiplier and current active fleet stats
    resource function get surge\-factor() returns json|http:InternalServerError {
        do {
            map<json>[] availableDrivers = check queryCollection(driversCollection, {"status": "AVAILABLE"});
            map<json>[] busyDrivers = check queryCollection(driversCollection, {"status": "BUSY"});

            int availCount = availableDrivers.length();
            int busyCount = busyDrivers.length();
            float surgeMultiplier = 1.0;

            if availCount == 0 && busyCount > 0 {
                surgeMultiplier = 1.8; // Peak scarcity
            } else if availCount > 0 && busyCount > availCount {
                surgeMultiplier = 1.4; // High demand
            } else if availCount > 0 && busyCount > 0 {
                surgeMultiplier = 1.15; // Moderate demand
            }

            return {
                "status": "success",
                "surgeMultiplier": surgeMultiplier,
                "availableDrivers": availCount,
                "busyDrivers": busyCount,
                "totalFleet": availCount + busyCount,
                "demandLevel": surgeMultiplier > 1.3 ? "HIGH" : (surgeMultiplier > 1.0 ? "MODERATE" : "NORMAL")
            };
        } on fail error e {
            log:printError("Failed to compute surge factor", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }
}

// ──────────────────────────────────────────────
// Kafka Consumer: Listen for Orders Ready for Pickup
// Automatically creates delivery assignment when an order is marked READY
// ──────────────────────────────────────────────
listener kafka:Listener deliveryKafkaListener = new (kafkaBootstrapServers, {
    groupId: "delivery-service-group",
    topics: ["orders.ready"],
    pollingInterval: 1
});

service on deliveryKafkaListener {

    # Automatically coordinates driver assignment when orders are READY
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            do {
                string messageStr = check string:fromBytes(rec.value);
                json message = check messageStr.fromJsonString();

                log:printInfo("Received orders.ready event for automated driver dispatch");

                string orderId = check message._id.ensureType();
                string restaurantId = check message.restaurantId.ensureType();
                string customerId = check message.customerId.ensureType();
                string deliveryAddress = check message.deliveryAddress.ensureType();

                // Check if delivery already assigned for this order
                map<json> existingFilter = {"orderId": orderId};
                map<json>[] existing = check queryCollection(deliveriesCollection, existingFilter);
                if existing.length() > 0 {
                    log:printInfo("Delivery already exists for ready order", orderId = orderId);
                    continue;
                }

                // Check available drivers
                map<json> availFilter = {"status": "AVAILABLE"};
                map<json>[] availDrivers = check queryCollection(driversCollection, availFilter);

                string driverId = "";
                string driverName = "Standby Driver";
                string driverPhone = "+264 81 123 4567";
                float initialLat = -22.5600;
                float initialLon = 17.0650;

                if availDrivers.length() > 0 {
                    map<json> chosen = availDrivers[0];
                    driverId = check chosen["_id"].ensureType();
                    driverName = check chosen["name"].ensureType();
                    driverPhone = check chosen["phone"].ensureType();
                    if chosen["currentLatitude"] is float {
                        initialLat = <float>chosen["currentLatitude"];
                        initialLon = <float>chosen["currentLongitude"];
                    }

                    // Mark driver as BUSY
                    string curTime = time:utcToString(time:utcNow());
                    mongodb:Update dUp = {set: {"status": "BUSY", "updatedAt": curTime}};
                    _ = check driversCollection->updateOne({"_id": driverId}, dUp);
                }

                string deliveryId = uuid:createType1AsString();
                string currentTime = time:utcToString(time:utcNow());
                float destLat = -22.5750;
                float destLon = 17.0850;
                if message is map<json> {
                    json latJson = message["deliveryLatitude"];
                    if latJson is float {
                        destLat = latJson;
                    }
                    json lonJson = message["deliveryLongitude"];
                    if lonJson is float {
                        destLon = lonJson;
                    }
                }

                float dist = calculateDistance(initialLat, initialLon, destLat, destLon);
                int eta = calculateEta(dist);

                map<json> newDelivery = {
                    "_id": deliveryId,
                    "orderId": orderId,
                    "driverId": driverId,
                    "driverName": driverName,
                    "driverPhone": driverPhone,
                    "restaurantId": restaurantId,
                    "customerId": customerId,
                    "status": "ASSIGNED",
                    "deliveryAddress": deliveryAddress,
                    "pickupLatitude": initialLat,
                    "pickupLongitude": initialLon,
                    "destinationLatitude": destLat,
                    "destinationLongitude": destLon,
                    "currentLatitude": initialLat,
                    "currentLongitude": initialLon,
                    "distanceKm": dist,
                    "estimatedMinutes": eta,
                    "progressPercent": 15,
                    "assignedAt": currentTime,
                    "pickedUpAt": (),
                    "deliveredAt": (),
                    "updatedAt": currentTime
                };

                check deliveriesCollection->insertOne(newDelivery);
                check publishDeliveryEvent(TOPIC_DELIVERY_ASSIGNED, newDelivery);

                log:printInfo("Auto-assigned delivery for ready order", orderId = orderId, driverId = driverId);

            } on fail error e {
                log:printError("Error auto-dispatching delivery", 'error = e);
            }
        }
    }
}
