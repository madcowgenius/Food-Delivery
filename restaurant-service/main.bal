import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerina/time;
import ballerinax/mongodb;

// ──────────────────────────────────────────────
// Configurable Variables
// ──────────────────────────────────────────────
configurable int servicePort = 8082;
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string databaseName = "restaurant_db";

// ──────────────────────────────────────────────
// Record Types
// ──────────────────────────────────────────────

# Represents a restaurant in the system.
type Restaurant record {|
    string _id;
    string name;
    string address;
    string phone;
    string cuisine;
    float rating;
    boolean isOpen;
    string createdAt;
    string updatedAt;
|};

# Request payload for registering a new restaurant.
type RestaurantRequest record {|
    string name;
    string address;
    string phone;
    string cuisine;
|};

# Represents a menu item belonging to a restaurant.
type MenuItem record {|
    string _id;
    string restaurantId;
    string name;
    string description;
    float price;
    string category;
    boolean available;
    string imageUrl?;
|};

# Request payload for adding a menu item.
type MenuItemRequest record {|
    string name;
    string description;
    float price;
    string category;
    string imageUrl?;
|};

# Opening hours for a restaurant.
type OpeningHours record {|
    string _id;
    string restaurantId;
    string day;
    string openTime;
    string closeTime;
|};

# Request payload for setting opening hours.
type OpeningHoursRequest record {|
    string day;
    string openTime;
    string closeTime;
|};

// ──────────────────────────────────────────────
// MongoDB Initialization
// ──────────────────────────────────────────────
final mongodb:Client mongoClient;
final mongodb:Database db;

const string RESTAURANTS_COLLECTION = "restaurants";
const string MENU_ITEMS_COLLECTION = "menu_items";
const string OPENING_HOURS_COLLECTION = "opening_hours";

final mongodb:Collection restaurantsCollection;
final mongodb:Collection menuItemsCollection;
final mongodb:Collection openingHoursCollection;

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
    restaurantsCollection = check db->getCollection(RESTAURANTS_COLLECTION);
    menuItemsCollection = check db->getCollection(MENU_ITEMS_COLLECTION);
    openingHoursCollection = check db->getCollection(OPENING_HOURS_COLLECTION);
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
        return {"status": "UP", "service": "restaurant-service"};
    }

    # Register a new restaurant.
    # + payload - Restaurant details
    # + return - Created restaurant or error
    resource function post restaurants(@http:Payload RestaurantRequest payload) returns http:Created|http:InternalServerError {
        do {
            string restaurantId = uuid:createType1AsString();
            string currentTime = time:utcToString(time:utcNow());

            map<json> restaurantDoc = {
                "_id": restaurantId,
                "name": payload.name,
                "address": payload.address,
                "phone": payload.phone,
                "cuisine": payload.cuisine,
                "rating": 0.0,
                "isOpen": false,
                "createdAt": currentTime,
                "updatedAt": currentTime
            };

            check restaurantsCollection->insertOne(restaurantDoc);

            log:printInfo("Restaurant registered", restaurantId = restaurantId);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Restaurant registered successfully",
                    "data": restaurantDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to register restaurant", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve all restaurants.
    # + return - List of all restaurants
    resource function get restaurants() returns json|http:InternalServerError {
        do {
            map<json>[] restaurants = check queryCollection(restaurantsCollection, {});

            return {
                "status": "success",
                "count": restaurants.length(),
                "data": restaurants
            };
        } on fail error e {
            log:printError("Failed to retrieve restaurants", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve a restaurant by ID.
    # + restaurantId - The unique restaurant identifier
    # + return - Restaurant details or not found
    resource function get restaurants/[string restaurantId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": restaurantId};
            map<json>[] results = check queryCollection(restaurantsCollection, filter);

            if results.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Restaurant not found"}
                };
                return notFound;
            }

            return {
                "status": "success",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to retrieve restaurant", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Update a restaurant.
    # + restaurantId - The unique restaurant identifier
    # + payload - Updated restaurant fields
    # + return - Updated restaurant or error
    resource function put restaurants/[string restaurantId](@http:Payload json payload) returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": restaurantId};
            string currentTime = time:utcToString(time:utcNow());

            map<json> payloadMap = check payload.ensureType();
            payloadMap["updatedAt"] = currentTime;

            mongodb:Update update = {set: payloadMap};
            mongodb:UpdateResult res = check restaurantsCollection->updateOne(filter, update);

            if res.matchedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Restaurant not found"}
                };
                return notFound;
            }

            map<json>[] results = check queryCollection(restaurantsCollection, filter);
            return {
                "status": "success",
                "message": "Restaurant updated successfully",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to update restaurant", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Delete a restaurant and all associated data.
    # + restaurantId - The unique restaurant identifier
    # + return - Success or error
    resource function delete restaurants/[string restaurantId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": restaurantId};
            mongodb:DeleteResult res = check restaurantsCollection->deleteOne(filter);

            if res.deletedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Restaurant not found"}
                };
                return notFound;
            }

            // Clean up related data
            map<json> relatedFilter = {"restaurantId": restaurantId};
            _ = check menuItemsCollection->deleteMany(relatedFilter);
            _ = check openingHoursCollection->deleteMany(relatedFilter);

            log:printInfo("Restaurant deleted", restaurantId = restaurantId);

            return {
                "status": "success",
                "message": "Restaurant and related data deleted"
            };
        } on fail error e {
            log:printError("Failed to delete restaurant", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Toggle restaurant open/closed status.
    # + restaurantId - The unique restaurant identifier
    # + return - Updated status or error
    resource function put restaurants/[string restaurantId]/status(@http:Payload json payload) returns json|http:NotFound|http:InternalServerError {
        do {
            boolean isOpen = check payload.isOpen;
            map<json> filter = {"_id": restaurantId};
            string currentTime = time:utcToString(time:utcNow());

            mongodb:Update update = {set: {"isOpen": isOpen, "updatedAt": currentTime}};
            mongodb:UpdateResult res = check restaurantsCollection->updateOne(filter, update);

            if res.matchedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Restaurant not found"}
                };
                return notFound;
            }

            string statusStr = isOpen ? "open" : "closed";
            log:printInfo("Restaurant status updated", restaurantId = restaurantId, status = statusStr);

            return {
                "status": "success",
                "message": "Restaurant is now " + statusStr
            };
        } on fail error e {
            log:printError("Failed to update restaurant status", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    // ─── Menu Item Endpoints ─────────────────

    # Add a menu item to a restaurant.
    # + restaurantId - The unique restaurant identifier
    # + payload - Menu item details
    # + return - Created menu item or error
    resource function post restaurants/[string restaurantId]/menu(@http:Payload MenuItemRequest payload) returns http:Created|http:NotFound|http:InternalServerError {
        do {
            map<json> restaurantFilter = {"_id": restaurantId};
            map<json>[] restaurants = check queryCollection(restaurantsCollection, restaurantFilter);
            if restaurants.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Restaurant not found"}
                };
                return notFound;
            }

            string menuItemId = uuid:createType1AsString();
            map<json> menuItemDoc = {
                "_id": menuItemId,
                "restaurantId": restaurantId,
                "name": payload.name,
                "description": payload.description,
                "price": payload.price,
                "category": payload.category,
                "available": true
            };

            if payload.imageUrl is string {
                menuItemDoc["imageUrl"] = <string>payload.imageUrl;
            }

            check menuItemsCollection->insertOne(menuItemDoc);

            log:printInfo("Menu item added", restaurantId = restaurantId, menuItemId = menuItemId);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Menu item added",
                    "data": menuItemDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to add menu item", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve the full menu for a restaurant.
    # + restaurantId - The unique restaurant identifier
    # + return - List of menu items
    resource function get restaurants/[string restaurantId]/menu() returns json|http:InternalServerError {
        do {
            map<json> filter = {"restaurantId": restaurantId};
            map<json>[] menuItems = check queryCollection(menuItemsCollection, filter);

            return {
                "status": "success",
                "count": menuItems.length(),
                "data": menuItems
            };
        } on fail error e {
            log:printError("Failed to retrieve menu", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Update a menu item.
    # + restaurantId - The unique restaurant identifier
    # + itemId - The unique menu item identifier
    # + payload - Updated menu item fields
    # + return - Updated menu item or error
    resource function put restaurants/[string restaurantId]/menu/[string itemId](@http:Payload json payload) returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": itemId, "restaurantId": restaurantId};
            map<json> payloadMap = check payload.ensureType();
            mongodb:Update update = {set: payloadMap};
            mongodb:UpdateResult res = check menuItemsCollection->updateOne(filter, update);

            if res.matchedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Menu item not found"}
                };
                return notFound;
            }

            map<json>[] results = check queryCollection(menuItemsCollection, filter);
            return {
                "status": "success",
                "message": "Menu item updated",
                "data": results[0]
            };
        } on fail error e {
            log:printError("Failed to update menu item", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Delete a menu item from a restaurant.
    # + restaurantId - The unique restaurant identifier
    # + itemId - The unique menu item identifier
    # + return - Success or error
    resource function delete restaurants/[string restaurantId]/menu/[string itemId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json> filter = {"_id": itemId, "restaurantId": restaurantId};
            mongodb:DeleteResult res = check menuItemsCollection->deleteOne(filter);

            if res.deletedCount == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Menu item not found"}
                };
                return notFound;
            }

            return {"status": "success", "message": "Menu item deleted"};
        } on fail error e {
            log:printError("Failed to delete menu item", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    // ─── Opening Hours Endpoints ─────────────

    # Set opening hours for a restaurant.
    # + restaurantId - The unique restaurant identifier
    # + payload - Opening hours details
    # + return - Created hours entry or error
    resource function post restaurants/[string restaurantId]/hours(@http:Payload OpeningHoursRequest payload) returns http:Created|http:InternalServerError {
        do {
            string hoursId = uuid:createType1AsString();
            map<json> hoursDoc = {
                "_id": hoursId,
                "restaurantId": restaurantId,
                "day": payload.day,
                "openTime": payload.openTime,
                "closeTime": payload.closeTime
            };

            check openingHoursCollection->insertOne(hoursDoc);

            http:Created response = {
                body: {
                    "status": "success",
                    "message": "Opening hours set",
                    "data": hoursDoc
                }
            };
            return response;
        } on fail error e {
            log:printError("Failed to set opening hours", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Retrieve opening hours for a restaurant.
    # + restaurantId - The unique restaurant identifier
    # + return - Opening hours data
    resource function get restaurants/[string restaurantId]/hours() returns json|http:InternalServerError {
        do {
            map<json> filter = {"restaurantId": restaurantId};
            map<json>[] hours = check queryCollection(openingHoursCollection, filter);

            return {
                "status": "success",
                "data": hours
            };
        } on fail error e {
            log:printError("Failed to retrieve opening hours", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }
}
