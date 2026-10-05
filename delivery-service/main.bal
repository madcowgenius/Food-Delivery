import ballerina/http;
import ballerina/time;
import ballerina:uuid;
import ballerinax/kafka;

final kafka:Producer producer = check new (kafka:DEFAULT_URL);

type DeliveryRequest record {|
    string orderId;
    string restaurantAddress;
    string customerAddress;
|};

map<string> assignments = {};

service /deliveries on new http:Listener(8085) {
    resource function post .(DeliveryRequest req) returns json|error {
        string deliveryId = uuid:createType1AsString();
        assignments[req.orderId] = deliveryId;
        json event = {
            deliveryId: deliveryId,
            orderId: req.orderId,
            eventType: "DELIVERY_ASSIGNED",
            timestamp: time:utcToString(time:utcNow())
        };
        check producer->send({
            topic: "delivery.assigned",
            key: req.orderId.toBytes(),
            value: event.toJsonString().toBytes()
        });
        return {deliveryId: deliveryId, orderId: req.orderId, status: "ASSIGNED"};
    }

    resource function put [string orderId]/status (json status) returns json|error {
        if !assignments.hasKey(orderId) {
            return error("Delivery not found");
        }
        return {orderId: orderId, status: status};
    }
}
