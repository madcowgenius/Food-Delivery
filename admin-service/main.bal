import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;

// ──────────────────────────────────────────────
// Configurable Variables
// ──────────────────────────────────────────────
configurable int servicePort = 8087;
configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string orderServiceUrl = "http://localhost:8083";
configurable string customerServiceUrl = "http://localhost:8081";
configurable string restaurantServiceUrl = "http://localhost:8082";
configurable string paymentServiceUrl = "http://localhost:8084";
configurable string deliveryServiceUrl = "http://localhost:8085";

// ──────────────────────────────────────────────
// MongoDB Clients & Collections
// ──────────────────────────────────────────────
final mongodb:Client mongoClient;
final mongodb:Database orderDb;
final mongodb:Database restaurantDb;
final mongodb:Database deliveryDb;
final mongodb:Database customerDb;
final mongodb:Database paymentDb;

final mongodb:Collection ordersCol;
final mongodb:Collection restaurantsCol;
final mongodb:Collection driversCol;
final mongodb:Collection deliveriesCol;
final mongodb:Collection customersCol;
final mongodb:Collection paymentsCol;

function init() returns error? {
    mongoClient = check new ({
        connection: {
            serverAddress: {
                host: mongoHost,
                port: mongoPort
            }
        }
    });

    orderDb = check mongoClient->getDatabase("order_db");
    restaurantDb = check mongoClient->getDatabase("restaurant_db");
    deliveryDb = check mongoClient->getDatabase("delivery_db");
    customerDb = check mongoClient->getDatabase("customer_db");
    paymentDb = check mongoClient->getDatabase("payment_db");

    ordersCol = check orderDb->getCollection("orders");
    restaurantsCol = check restaurantDb->getCollection("restaurants");
    driversCol = check deliveryDb->getCollection("drivers");
    deliveriesCol = check deliveryDb->getCollection("deliveries");
    customersCol = check customerDb->getCollection("customers");
    paymentsCol = check paymentDb->getCollection("payments");
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
        return {"status": "UP", "service": "admin-service"};
    }

    # High-level platform executive dashboard.
    # Aggregates overall SME ecosystem statistics across orders, revenue, drivers, and restaurants.
    # + return - System summary metrics
    resource function get admin/dashboard() returns json|http:InternalServerError {
        do {
            map<json>[] allOrders = check queryCollection(ordersCol, {});
            map<json>[] allCustomers = check queryCollection(customersCol, {});
            map<json>[] allRestaurants = check queryCollection(restaurantsCol, {});
            map<json>[] allDrivers = check queryCollection(driversCol, {});
            map<json>[] allDeliveries = check queryCollection(deliveriesCol, {});

            int totalOrders = allOrders.length();
            int completedOrders = 0;
            int cancelledOrders = 0;
            int activeOrders = 0;
            float totalRevenue = 0.0;

            foreach map<json> o in allOrders {
                string status = "";
                if o["status"] is string {
                    status = <string>o["status"];
                }

                if status == "DELIVERED" {
                    completedOrders += 1;
                    if o["totalAmount"] is float {
                        totalRevenue += <float>o["totalAmount"];
                    }
                } else if status == "CANCELLED" {
                    cancelledOrders += 1;
                } else {
                    activeOrders += 1;
                }
            }

            int availableDrivers = 0;
            int busyDrivers = 0;
            foreach map<json> d in allDrivers {
                if d["status"] is string && <string>d["status"] == "AVAILABLE" {
                    availableDrivers += 1;
                } else if d["status"] is string && <string>d["status"] == "BUSY" {
                    busyDrivers += 1;
                }
            }

            float avgOrderValue = completedOrders > 0 ? (totalRevenue / <float>completedOrders) : 0.0;
            float platformCommission = totalRevenue * 0.10; // 10% platform SME commission fee

            return {
                "status": "success",
                "timestamp": time:utcToString(time:utcNow()),
                "metrics": {
                    "totalCustomers": allCustomers.length(),
                    "totalRestaurants": allRestaurants.length(),
                    "totalDrivers": allDrivers.length(),
                    "activeFleet": {
                        "available": availableDrivers,
                        "busy": busyDrivers
                    },
                    "orders": {
                        "total": totalOrders,
                        "completed": completedOrders,
                        "active": activeOrders,
                        "cancelled": cancelledOrders,
                        "completionRate": totalOrders > 0 ? (<float>completedOrders / <float>totalOrders * 100.0) : 0.0
                    },
                    "financials": {
                        "grossMerchandiseVolume": totalRevenue,
                        "platformCommissionEarned": platformCommission,
                        "restaurantPayouts": totalRevenue - platformCommission,
                        "averageOrderValue": avgOrderValue
                    },
                    "deliveries": {
                        "total": allDeliveries.length()
                    }
                }
            };
        } on fail error e {
            log:printError("Failed to generate admin dashboard", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Restaurant Performance Reports.
    # Breaks down sales, order volume, and ratings for every restaurant.
    # + return - Restaurant performance statistics
    resource function get admin/reports/restaurants() returns json|http:InternalServerError {
        do {
            map<json>[] restaurants = check queryCollection(restaurantsCol, {});
            map<json>[] orders = check queryCollection(ordersCol, {});

            json[] restaurantReports = [];

            foreach map<json> r in restaurants {
                string restId = check r["_id"].ensureType();
                string restName = check r["name"].ensureType();
                string cuisine = check r["cuisine"].ensureType();
                float rating = 0.0;
                if r["rating"] is float {
                    rating = <float>r["rating"];
                }
                boolean isOpen = false;
                if r["isOpen"] is boolean {
                    isOpen = <boolean>r["isOpen"];
                }

                int restOrderCount = 0;
                int completedCount = 0;
                float restTotalSales = 0.0;

                foreach map<json> o in orders {
                    if o["restaurantId"] is string && <string>o["restaurantId"] == restId {
                        restOrderCount += 1;
                        if o["status"] is string && <string>o["status"] == "DELIVERED" {
                            completedCount += 1;
                            if o["totalAmount"] is float {
                                restTotalSales += <float>o["totalAmount"];
                            }
                        }
                    }
                }

                float avgValue = completedCount > 0 ? (restTotalSales / <float>completedCount) : 0.0;

                restaurantReports.push({
                    "restaurantId": restId,
                    "name": restName,
                    "cuisine": cuisine,
                    "rating": rating,
                    "isOpen": isOpen,
                    "totalOrders": restOrderCount,
                    "completedOrders": completedCount,
                    "grossSales": restTotalSales,
                    "averageOrderValue": avgValue,
                    "commissionPaid": restTotalSales * 0.10,
                    "netEarnings": restTotalSales * 0.90
                });
            }

            return {
                "status": "success",
                "timestamp": time:utcToString(time:utcNow()),
                "count": restaurantReports.length(),
                "data": restaurantReports
            };
        } on fail error e {
            log:printError("Failed to generate restaurant report", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Specific Restaurant Detailed Report.
    # + restaurantId - The unique restaurant identifier
    # + return - Restaurant deep-dive statistics
    resource function get admin/reports/restaurants/[string restaurantId]() returns json|http:NotFound|http:InternalServerError {
        do {
            map<json>[] rRes = check queryCollection(restaurantsCol, {"_id": restaurantId});
            if rRes.length() == 0 {
                http:NotFound notFound = {
                    body: {"status": "error", "message": "Restaurant not found"}
                };
                return notFound;
            }

            map<json> rest = rRes[0];
            map<json>[] restOrders = check queryCollection(ordersCol, {"restaurantId": restaurantId});

            int totalOrders = restOrders.length();
            int completed = 0;
            float totalSales = 0.0;

            foreach map<json> o in restOrders {
                if o["status"] is string && <string>o["status"] == "DELIVERED" {
                    completed += 1;
                    if o["totalAmount"] is float {
                        totalSales += <float>o["totalAmount"];
                    }
                }
            }

            return {
                "status": "success",
                "restaurant": rest,
                "summary": {
                    "totalOrders": totalOrders,
                    "completedOrders": completed,
                    "grossSales": totalSales,
                    "averageOrderValue": completed > 0 ? (totalSales / <float>completed) : 0.0
                },
                "recentOrders": restOrders
            };
        } on fail error e {
            log:printError("Failed to retrieve single restaurant report", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Delivery & Driver Performance Reports.
    # Tracks driver delivery counts, active status, ratings, and fleet efficiency.
    # + return - Driver delivery performance
    resource function get admin/reports/deliveries() returns json|http:InternalServerError {
        do {
            map<json>[] drivers = check queryCollection(driversCol, {});
            map<json>[] deliveries = check queryCollection(deliveriesCol, {});

            int totalDeliveries = deliveries.length();
            int completedDeliveries = 0;
            int inTransitDeliveries = 0;
            float totalDeliveryKm = 0.0;

            foreach map<json> del in deliveries {
                if del["status"] is string {
                    string s = <string>del["status"];
                    if s == "DELIVERED" {
                        completedDeliveries += 1;
                    } else if s == "IN_TRANSIT" || s == "PICKED_UP" || s == "ASSIGNED" {
                        inTransitDeliveries += 1;
                    }
                }
                if del["distanceKm"] is float {
                    totalDeliveryKm += <float>del["distanceKm"];
                }
            }

            json[] driverLeaderboard = [];
            foreach map<json> drv in drivers {
                string dId = check drv["_id"].ensureType();
                string dName = check drv["name"].ensureType();
                string dPhone = check drv["phone"].ensureType();
                string vehicle = check drv["vehicleType"].ensureType();
                string status = check drv["status"].ensureType();
                float rating = 5.0;
                if drv["rating"] is float {
                    rating = <float>drv["rating"];
                }
                int totalDel = 0;
                if drv["totalDeliveries"] is int {
                    totalDel = <int>drv["totalDeliveries"];
                }

                driverLeaderboard.push({
                    "driverId": dId,
                    "name": dName,
                    "phone": dPhone,
                    "vehicleType": vehicle,
                    "status": status,
                    "rating": rating,
                    "completedDeliveries": totalDel
                });
            }

            return {
                "status": "success",
                "timestamp": time:utcToString(time:utcNow()),
                "deliverySummary": {
                    "totalDeliveries": totalDeliveries,
                    "completedDeliveries": completedDeliveries,
                    "inTransitDeliveries": inTransitDeliveries,
                    "totalKilometersTraveled": totalDeliveryKm,
                    "averageDistanceKm": totalDeliveries > 0 ? (totalDeliveryKm / <float>totalDeliveries) : 0.0
                },
                "driverLeaderboard": driverLeaderboard
            };
        } on fail error e {
            log:printError("Failed to generate delivery performance report", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Financial & Payment Settlement Reports.
    # Reports on completed payments, refunds, failed attempts, and platform revenue.
    # + return - Financial breakdown
    resource function get admin/reports/financial() returns json|http:InternalServerError {
        do {
            map<json>[] payments = check queryCollection(paymentsCol, {});

            int totalTransactions = payments.length();
            int completedCount = 0;
            int failedCount = 0;
            int refundedCount = 0;
            float totalVolume = 0.0;
            float totalRefunded = 0.0;

            foreach map<json> p in payments {
                if p["status"] is string {
                    string s = <string>p["status"];
                    if s == "COMPLETED" {
                        completedCount += 1;
                        if p["amount"] is float {
                            totalVolume += <float>p["amount"];
                        }
                    } else if s == "FAILED" {
                        failedCount += 1;
                    } else if s == "REFUNDED" {
                        refundedCount += 1;
                        if p["amount"] is float {
                            totalRefunded += <float>p["amount"];
                        }
                    }
                }
            }

            float platformCommission = totalVolume * 0.10;
            float netPayouts = totalVolume - platformCommission - totalRefunded;

            return {
                "status": "success",
                "timestamp": time:utcToString(time:utcNow()),
                "totalTransactions": totalTransactions,
                "completedCount": completedCount,
                "failedCount": failedCount,
                "refundedCount": refundedCount,
                "successRate": totalTransactions > 0 ? (<float>completedCount / <float>totalTransactions * 100.0) : 0.0,
                "financials": {
                    "grossTransactionVolume": totalVolume,
                    "refundedVolume": totalRefunded,
                    "netPlatformRevenue": platformCommission,
                    "netVendorPayouts": netPayouts
                }
            };
        } on fail error e {
            log:printError("Failed to generate financial report", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Order Lifecycle State Distribution.
    # Visualizes breakdown of order lifecycle states: CREATED, CONFIRMED, PREPARING, READY, OUT_FOR_DELIVERY, DELIVERED, CANCELLED.
    # + return - Distribution of order statuses
    resource function get admin/reports/orders/lifecycle() returns json|http:InternalServerError {
        do {
            map<json>[] orders = check queryCollection(ordersCol, {});

            int created = 0;
            int confirmed = 0;
            int preparing = 0;
            int ready = 0;
            int outForDelivery = 0;
            int delivered = 0;
            int cancelled = 0;

            foreach map<json> o in orders {
                if o["status"] is string {
                    string s = <string>o["status"];
                    if s == "CREATED" {
                        created += 1;
                    } else if s == "CONFIRMED" {
                        confirmed += 1;
                    } else if s == "PREPARING" {
                        preparing += 1;
                    } else if s == "READY" {
                        ready += 1;
                    } else if s == "OUT_FOR_DELIVERY" {
                        outForDelivery += 1;
                    } else if s == "DELIVERED" {
                        delivered += 1;
                    } else if s == "CANCELLED" {
                        cancelled += 1;
                    }
                }
            }

            return {
                "status": "success",
                "totalOrders": orders.length(),
                "breakdown": {
                    "CREATED": created,
                    "CONFIRMED": confirmed,
                    "PREPARING": preparing,
                    "READY": ready,
                    "OUT_FOR_DELIVERY": outForDelivery,
                    "DELIVERED": delivered,
                    "CANCELLED": cancelled
                }
            };
        } on fail error e {
            log:printError("Failed to generate order lifecycle report", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }

    # Live Surge Pricing Multiplier Analytics (Bonus: Surge Pricing).
    # Calculates dynamic pricing multiplier based on ratio of active orders to available drivers.
    # + return - Current surge multiplier and market demand analytics
    resource function get admin/surge\-pricing() returns json|http:InternalServerError {
        do {
            map<json>[] availDrivers = check queryCollection(driversCol, {"status": "AVAILABLE"});
            map<json>[] busyDrivers = check queryCollection(driversCol, {"status": "BUSY"});
            map<json>[] pendingOrders = check queryCollection(ordersCol, {"status": "CONFIRMED"});

            int available = availDrivers.length();
            int busy = busyDrivers.length();
            int pending = pendingOrders.length();

            float multiplier = 1.0;
            if available == 0 && (busy > 0 || pending > 0) {
                multiplier = 1.75;
            } else if available > 0 && pending > (available * 2) {
                multiplier = 1.5;
            } else if available > 0 && busy > available {
                multiplier = 1.25;
            }

            return {
                "status": "success",
                "timestamp": time:utcToString(time:utcNow()),
                "surgeMultiplier": multiplier,
                "marketCondition": multiplier >= 1.5 ? "SURGE_ACTIVE" : (multiplier > 1.0 ? "MODERATE_DEMAND" : "STANDARD_PRICING"),
                "fleet": {
                    "availableDrivers": available,
                    "busyDrivers": busy,
                    "pendingKitchenOrders": pending
                }
            };
        } on fail error e {
            log:printError("Failed to calculate surge pricing analytics", 'error = e);
            http:InternalServerError errorResponse = {
                body: {"status": "error", "message": e.message()}
            };
            return errorResponse;
        }
    }
}
