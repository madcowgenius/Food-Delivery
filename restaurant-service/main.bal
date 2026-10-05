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
final mongodb:Collection restaurantsCol = check db->getCollection("restaurants");
final mongodb:Collection menuItemsCol = check db->getCollection("menu_items");

// Record types
type Restaurant record {|
    string id;
    string name;
    string address;
    string phone;
    string openingHours;
    string closingHours;
    boolean isOpen;
    string createdAt;
|};

type RestaurantRequest record {|
    string name;
    string address;
    string phone;
    string openingHours;
    string closingHours;
|};

type MenuItem record {|
    string id;
    string restaurantId;
    string name;
    string description;
    decimal price;
    int stock;
    string category;
    boolean available;
|};

type MenuItemRequest record {|
    string restaurantId;
    string name;
    string description;
    decimal price;
    int stock;
    string category;
|};

service /restaurants on new http:Listener(8083) {

    // Create a new restaurant
    resource function post .(RestaurantRequest req) returns json|http:InternalServerError {
        string id = uuid:createType1AsString();
        Restaurant restaurant = {
            id: id,
            name: req.name,
            address: req.address,
            phone: req.phone,
            openingHours: req.openingHours,
            closingHours: req.closingHours,
            isOpen: true,
            createdAt: "2026-10-05T00:00:00Z"
        };
        map<json> doc = {
            "id": restaurant.id,
            "name": restaurant.name,
            "address": restaurant.address,
            "phone": restaurant.phone,
            "openingHours": restaurant.openingHours,
            "closingHours": restaurant.closingHours,
            "isOpen": restaurant.isOpen,
            "createdAt": restaurant.createdAt
        };
        mongodb:Error? insertResult = restaurantsCol->insertOne(doc);
        if insertResult is mongodb:Error {
            log:printError("Failed to insert restaurant", insertResult);
            return <http:InternalServerError>{body: {message: "Failed to create restaurant"}};
        }
        log:printInfo("Restaurant created: " + id);
        return restaurant;
    }

    // Get all restaurants
    resource function get .() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = restaurantsCol->find({});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch restaurants"}};
        }
        json[] restaurants = [];
        error? e = result.forEach(function(map<json> doc) {
            restaurants.push(doc);
        });
        if e is error {
            log:printError("Error iterating restaurants", e);
        }
        return restaurants;
    }

    // Get restaurant by ID
    resource function get [string id]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? result = restaurantsCol->findOne({"id": id});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if result is () {
            return <http:NotFound>{body: {message: "Restaurant not found", id: id}};
        }
        return result;
    }

    // Update restaurant
    resource function put [string id](RestaurantRequest req) returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = restaurantsCol->findOne({"id": id});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Restaurant not found", id: id}};
        }
        map<json> updateFields = {
            "name": req.name,
            "address": req.address,
            "phone": req.phone,
            "openingHours": req.openingHours,
            "closingHours": req.closingHours
        };
        mongodb:UpdateResult|mongodb:Error updateResult = restaurantsCol->updateOne({"id": id}, {"set": updateFields});
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to update restaurant"}};
        }
        map<json>|mongodb:Error? updated = restaurantsCol->findOne({"id": id});
        if updated is map<json> {
            return updated;
        }
        return <http:InternalServerError>{body: {message: "Failed to fetch updated restaurant"}};
    }

    // Toggle restaurant open/close status
    resource function put [string id]/status(json statusReq) returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = restaurantsCol->findOne({"id": id});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Restaurant not found", id: id}};
        }
        boolean|error isOpen = statusReq.isOpen.ensureType();
        if isOpen is error {
            return <http:InternalServerError>{body: {message: "Invalid status"}};
        }
        mongodb:UpdateResult|mongodb:Error updateResult = restaurantsCol->updateOne({"id": id}, {"set": {"isOpen": isOpen}});
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to update status"}};
        }
        return {id: id, isOpen: isOpen};
    }

    // ====== Menu Items ======

    // Add menu item
    resource function post [string restaurantId]/menu(MenuItemRequest req) returns json|http:InternalServerError {
        string id = uuid:createType1AsString();
        MenuItem item = {
            id: id,
            restaurantId: restaurantId,
            name: req.name,
            description: req.description,
            price: req.price,
            stock: req.stock,
            category: req.category,
            available: req.stock > 0
        };
        map<json> doc = {
            "id": item.id,
            "restaurantId": item.restaurantId,
            "name": item.name,
            "description": item.description,
            "price": item.price,
            "stock": item.stock,
            "category": item.category,
            "available": item.available
        };
        mongodb:Error? insertResult = menuItemsCol->insertOne(doc);
        if insertResult is mongodb:Error {
            log:printError("Failed to insert menu item", insertResult);
            return <http:InternalServerError>{body: {message: "Failed to add menu item"}};
        }
        return item;
    }

    // Get menu for a restaurant
    resource function get [string restaurantId]/menu() returns json|http:InternalServerError {
        stream<map<json>, error?>|mongodb:Error result = menuItemsCol->find({"restaurantId": restaurantId});
        if result is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to fetch menu"}};
        }
        json[] items = [];
        error? e = result.forEach(function(map<json> doc) {
            items.push(doc);
        });
        if e is error {
            log:printError("Error iterating menu items", e);
        }
        return items;
    }

    // Update menu item
    resource function put [string restaurantId]/menu/[string itemId](MenuItemRequest req) returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = menuItemsCol->findOne({"id": itemId, "restaurantId": restaurantId});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        map<json> updateFields = {
            "name": req.name,
            "description": req.description,
            "price": req.price,
            "stock": req.stock,
            "category": req.category,
            "available": req.stock > 0
        };
        mongodb:UpdateResult|mongodb:Error updateResult = menuItemsCol->updateOne({"id": itemId}, {"set": updateFields});
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to update menu item"}};
        }
        map<json>|mongodb:Error? updated = menuItemsCol->findOne({"id": itemId});
        if updated is map<json> {
            return updated;
        }
        return <http:InternalServerError>{body: {message: "Failed to fetch updated menu item"}};
    }

    // Delete menu item
    resource function delete [string restaurantId]/menu/[string itemId]() returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = menuItemsCol->findOne({"id": itemId, "restaurantId": restaurantId});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        mongodb:DeleteResult|mongodb:Error deleteResult = menuItemsCol->deleteOne({"id": itemId});
        if deleteResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to delete menu item"}};
        }
        return {message: "Menu item deleted", itemId: itemId};
    }

    // Decrement stock for a menu item (called when order is placed)
    resource function put menu/[string itemId]/decrement(json body) returns json|http:NotFound|http:InternalServerError {
        map<json>|mongodb:Error? existing = menuItemsCol->findOne({"id": itemId});
        if existing is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Database error"}};
        }
        if existing is () {
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        int|error qty = body.quantity.ensureType();
        int quantity = qty is error ? 1 : qty;
        mongodb:UpdateResult|mongodb:Error updateResult = menuItemsCol->updateOne(
            {"id": itemId},
            {"inc": {"stock": -quantity}}
        );
        if updateResult is mongodb:Error {
            return <http:InternalServerError>{body: {message: "Failed to decrement stock"}};
        }
        return {message: "Stock decremented", itemId: itemId, decrementedBy: quantity};
    }
}
