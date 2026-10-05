import ballerina/http;
import ballerina/time;

type NotificationRequest record {|
    string recipient;
    string channel;
    string message;
|};

service /notifications on new http:Listener(8086) {
    resource function post .(NotificationRequest req) returns json {
        return {
            status: "SENT",
            recipient: req.recipient,
            channel: req.channel,
            message: req.message,
            timestamp: time:utcToString(time:utcNow())
        };
    }
}
