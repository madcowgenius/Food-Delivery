import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerina/time;
import ballerinax/mongodb;

// ──────────────────────────────────────────────
// Configurable Variables
// ──────────────────────────────────────────────
configurable int servicePort = 8081;
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string databaseName = "customer_db";

// ──────────────────────────────────────────────
// Record Types
// ──────────────────────────────────────────────

# Represents a customer in the system.
type Customer record {|
    string _id;
    string name;
    string email;
    string phone;
    string createdAt;
    string updatedAt;
|};

# Request payload for creating a new customer.
type CustomerRequest record {|
    string name;
    string email;
    string phone;
|};

# Request payload for updating a customer.
type CustomerUpdateRequest record {|
    string name?;
    string email?;
    string phone?;
|};

# Represents a delivery address for a customer.
type Address record {|
    string _id;
    string customerId;
    string label;
    string street;
    string city;
    string state;
    string zipCode;
    float latitude?;
    float longitude?;
|};

# Request payload for adding a new address.
type AddressRequest record {|
    string label;
    string street;
    string city;
    string state;
    string zipCode;
    float latitude?;
    float longitude?;
|};

// ──────────────────────────────────────────────
// MongoDB Client Initialization
// ──────────────────────────────────────────────
final mongodb:Client mongoClient;
final mongodb:Database db;

const string CUSTOMERS_COLLECTION = "customers";
const string ADDRESSES_COLLECTION = "addresses";

final mongodb:Collection customersCollection;
final mongodb:Collection addressesCollection;

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
    customersCollection = check db->getCollection(CUSTOMERS_COLLECTION);
    addressesCollection = check db->getCollection(ADDRESSES_COLLECTION);
}

// Helper function to query a collection and return array of map<json>
isolated function queryCollection(mongodb:Collection collectionRef, map<json> filter = {}) returns map<json>[]|error {
    stream<map<json>, error?> docStream = check collectionRef->find(filter);
    map<json>[] results = check from var doc in docStream select doc;
    check docStream.close();
    return results;
}

// ──────────────────────────────────────────────
// HTTP REST API Service
// ──────────────────────────────────────────────
service /api/v1 on new http:Listener(servicePort) {

    # Health check endpoint.
    resource function get health() returns json {
        return {"status": "UP", "service": "customer-service"};
    }

    # Register a new customer.
    # + payload - Customer registration details
    # + return - Created customer or error
    resource function post customers(@http:Payload CustomerRequest payload) returns http:Created|http:BadRequest|http:InternalServerError {
        do {
            string customerId = uuid:createType1AsString();
            string currentTime = time:utcToString(time:utcNow());

            map<json> customerDoc = {
                "_id": customerId,
                "name": payload.name,
                "email": payload.email,
                "phone": payload.phone,
                "createdAt": currentTime,
                "updatedAt": currentTime
            };

            check customersCollection->insertOne(customerDoc);

            log:printInfo("Customer registered successfully", customerId = customerId);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Customer registered successfully",
                    "data": customerDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to register customer", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all customers.
    # + return - List of all customers
    resource function get customers() returns json|http:InternalServerError {
        do {
            map<json>[] customers = check queryCollection(customersCollection, {});

            return {
                "status": "success",
                "count": customers.length(),
                "data": customers
            };
        } on fail error e {
            log:printError("Failed to retrieve customers", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve a customer by ID.
    # + customerId - The unique customer identifier
    # + return - Customer details or not found
    resource function get customers/[string customerId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": customerId};
            map<json>[] results = check queryCollection(customersCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Customer not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve customer", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Update an existing customer.
    # + customerId - The unique customer identifier
    # + payload - Fields to update
    # + return - Updated customer or error
    resource function put customers/[string customerId](@http:Payload CustomerUpdateRequest payload) returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": customerId};
            string currentTime = time:utcToString(time:utcNow());

            map<json> updateFields = {"updatedAt": currentTime};
            if payload.name is string {
                updateFields["name"] = <string>payload.name;
            }
            if payload.email is string {
                updateFields["email"] = <string>payload.email;
            }
            if payload.phone is string {
                updateFields["phone"] = <string>payload.phone;
            }

            mongodb:Update update = {set: updateFields};
            mongodb:UpdateResult res = check customersCollection->updateOne(filter, update);

            if res.matchedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Customer not found"}
                };
                return notFound;
            }

            log:printInfo("Customer updated successfully", customerId = customerId);

            // Fetch and return updated document
            map<json>[] results = check queryCollection(customersCollection, filter);
            return {
                "status": "success",
                "message": "Customer updated successfully",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to update customer", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Delete a customer.
    # + customerId - The unique customer identifier
    # + return - Success or error
    resource function delete customers/[string customerId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": customerId};
            mongodb:DeleteResult res = check customersCollection->deleteOne(filter);

            if res.deletedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Customer not found"}
                };
                return notFound;
            }

            // Also delete associated addresses
            map<json> addressFilter = {"customerId": customerId};
            _ = check addressesCollection->deleteMany(addressFilter);

            log:printInfo("Customer deleted successfully", customerId = customerId);

            return {
                "status": "success",
                "message": "Customer and associated addresses deleted"
            };
        } on fail error e {
            log:printError("Failed to delete customer", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Add a delivery address for a customer.
    # + customerId - The unique customer identifier
    # + payload - Address details
    # + return - Created address or error
    resource function post customers/[string customerId]/addresses(@http:Payload AddressRequest payload) returns http:Created|http:NotFound|http:InternalServerError {
        do {
            // Verify customer exists
            map<json> customerFilter = {"_id": customerId};
            map<json>[] customers = check queryCollection(customersCollection, customerFilter);
            if customers.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Customer not found"}
                };
                return notFound;
            }

            string addressId = uuid:createType1AsString();
            map<json> addressDoc = {
                "_id": addressId,
                "customerId": customerId,
                "label": payload.label,
                "street": payload.street,
                "city": payload.city,
                "state": payload.state,
                "zipCode": payload.zipCode
            };

            // Add optional coordinates if provided
            if payload.latitude is float {
                addressDoc["latitude"] = <float>payload.latitude;
            }
            if payload.longitude is float {
                addressDoc["longitude"] = <float>payload.longitude;
            }

            check addressesCollection->insertOne(addressDoc);

            log:printInfo("Address added for customer", customerId = customerId, addressId = addressId);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Address added successfully",
                    "data": addressDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to add address", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all addresses for a customer.
    # + customerId - The unique customer identifier
    # + return - List of addresses
    resource function get customers/[string customerId]/addresses() returns json|http:InternalServerError {
        do {
            map<json> filter = {"customerId": customerId};
            map<json>[] addresses = check queryCollection(addressesCollection, filter);

            return {
                "status": "success",
                "count": addresses.length(),
                "data": addresses
            };
        } on fail error e {
            log:printError("Failed to retrieve addresses", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Delete an address for a customer.
    # + customerId - The unique customer identifier
    # + addressId - The unique address identifier
    # + return - Success or error
    resource function delete customers/[string customerId]/addresses/[string addressId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": addressId, "customerId": customerId};
            mongodb:DeleteResult res = check addressesCollection->deleteOne(filter);

            if res.deletedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Address not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "message": "Address deleted successfully"
            };
        } on fail error e {
            log:printError("Failed to delete address", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }
}
