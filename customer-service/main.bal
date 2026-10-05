import ballerina/http;
import ballerina/uuid;
import ballerina/log;
import ballerinax/mongodb;

// MongoDB configuration
configurable string mongoHost = "mongo";
configurable int mongoPort = 27017;

final mongodb:Client mongoClient = check new ({
    connection: {
        serverAddress: {
            host: mongoHost,
            port: mongoPort
        }
    }
});

final mongodb:Database db = check mongoClient->getDatabase("food_delivery");
final mongodb:Collection customersCol = check db->getCollection("customers");

// Record types
type Customer record {|
    string id;
    string name;
    string email;
    string phone;
    string[] addresses;
    string[] orderHistory;
    string createdAt;
|};

type CustomerRequest record {|
    string name;
    string email;
    string phone;
    string[] addresses = [];
|};

type CustomerUpdate record {|
    string name?;
    string email?;
    string phone?;
    string[] addresses?;
|};

service /customers on new http:Listener(8082) {

    // Create a new customer
    resource function post .(CustomerRequest req) returns json|http:InternalServerError {
        string id = uuid:createType1AsString();
        Customer customer = {
            id: id,
            name: req.name,
            email: req.email,
            phone: req.phone,
            addresses: req.addresses,
            orderHistory: [],
            createdAt: getCurrentTimestamp()
        };
        map<json> doc = customerToDoc(customer);
        mongodb:Error? insertResult = customersCol->insertOne(doc);
        if insertResult is mongodb:Error {
            log:printError("Failed to insert customer", insertResult);
            return <http:InternalServerError>{body: {message: "Failed to create customer"}};
        }
        log:printInfo("Customer created: " + id);
        return customer;
    }

    // Get all customers
    resource function get .() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = customersCol->find({});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch customers"}};
        }
        json[] customers = [];
        error? e = result.forEach(function(map<json> doc) {
            customers.push(docToCustomerJson(doc));
        });
        if e is error {
            log:printError("Error iterating customers", e);
        }
        return customers;
    }

    // Get customer by ID
    resource function get [string id]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = customersCol->findOne({"id": id});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Customer not found", id: id}};
        }
        return docToCustomerJson(result);
    }

    // Update customer
    resource function put [string id](CustomerUpdate req) returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = customersCol->findOne({"id": id});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Customer not found", id: id}};
        }
        map<json> updateFields = {};
        if req.name is string {
            updateFields["name"] = req.name;
        }
        if req.email is string {
            updateFields["email"] = req.email;
        }
        if req.phone is string {
            updateFields["phone"] = req.phone;
        }
        if req.addresses is string[] {
            updateFields["addresses"] = req.addresses.toJson();
        }
        mongodb:UpdateResult|mongodb:Error updateResult = customersCol->updateOne({"id": id}, {"set": updateFields});
        if updateResult is mongodb:Error {
            log:printError("Failed to update customer", updateResult);
            return <http:InternalServerError>{body: {message: "Failed to update customer"}};
        }
        // Return updated customer
        map<json>|mongodb:Error? updated = customersCol->findOne({"id": id});
        if updated is map<json> {
            return docToCustomerJson(updated);
        }
        return <http:InternalServerError>{body: {message: "Failed to fetch updated customer"}};
    }

    // Delete customer
    resource function delete [string id]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = customersCol->findOne({"id": id});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Customer not found", id: id}};
        }
        mongodb:DeleteResult|mongodb:Error deleteResult = customersCol->deleteOne({"id": id});
        if deleteResult is mongodb:Error {
            log:printError("Failed to delete customer", deleteResult);
            return <http:InternalServerError>{body: {message: "Failed to delete customer"}};
        }
        return {message: "Customer deleted", id: id};
    }

    // Add order to history
    resource function post [string id]/orders(json orderRef) returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = customersCol->findOne({"id": id});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Customer not found", id: id}};
        }
        string|error orderId = orderRef.orderId.ensureType();
        if orderId is error {
            return <http:InternalServerError>{body: {message: "Invalid order reference"}};
        }
        mongodb:UpdateResult|mongodb:Error updateResult = customersCol->updateOne(
            {"id": id},
            {"push": {"orderHistory": orderId}}
        );
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to update order history"}};
        }
        return {message: "Order added to history", customerId: id, orderId: orderId};
    }

    // Get customer order history
    resource function get [string id]/orders() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = customersCol->findOne({"id": id});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Customer not found", id: id}};
        }
        return {customerId: id, orderHistory: result["orderHistory"]};
    }
}

function customerToDoc(Customer c) returns map<json> {
    return {
        "id": c.id,
        "name": c.name,
        "email": c.email,
        "phone": c.phone,
        "addresses": c.addresses.toJson(),
        "orderHistory": c.orderHistory.toJson(),
        "createdAt": c.createdAt
    };
}

function docToCustomerJson(map<json> doc) returns json {
    return {
        id: doc["id"],
        name: doc["name"],
        email: doc["email"],
        phone: doc["phone"],
        addresses: doc["addresses"],
        orderHistory: doc["orderHistory"],
        createdAt: doc["createdAt"]
    };
}

function getCurrentTimestamp() returns string {
    return "2026-10-05T00:00:00Z";
}

