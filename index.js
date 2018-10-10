/**
 * Description:
 * Created by wangbin.
 * Date: 16-8-3
 * Time: 上午11:02
 */
var RabbitMQClient = require('./app/servers/RabbitMQClient');
var RabbitMQServer = require('./app/servers/RabbitMQServer');
var amqClient = new RabbitMQClient();
var amqServer = new RabbitMQServer();

exports.consumer_rpc = function (mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback) {
    amqServer.rpc(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback)
};
exports.consumer_task = function (mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback) {
    amqServer.worker(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback);
};
exports.consumer_fanout = function (mqUrlArr, exchangeName, durable, noAck, exclusive, reconnectTime, callback) {
    amqServer.fanout(mqUrlArr, exchangeName, durable, noAck, exclusive, reconnectTime, callback);
};
exports.consumer_direct = function (mqUrlArr, exchangeName, severities, durable, noAck, exclusive, reconnectTime, callback) {
    amqServer.direct(mqUrlArr, exchangeName, severities, durable, noAck, exclusive, reconnectTime, callback);
};
exports.consumer_topic = function (mqUrlArr, exchangeName, topicKey, durable, noAck, exclusive, reconnectTime, callback) {
    amqServer.topic(mqUrlArr, exchangeName, topicKey, durable, noAck, exclusive, reconnectTime, callback);
};
exports.consumer_receive = function (mqUrlArr, queueName, durable, noAck, reconnectTime, callback) {
    amqServer.receive(mqUrlArr, queueName, durable, noAck, reconnectTime, callback);
};

exports.producer_rpc = function (mqUrlArr, queueName, msg, exclusive, noAck, reconnectTime, callback) {
    amqClient.rpc(mqUrlArr, queueName, msg, exclusive, noAck, reconnectTime, callback);
};
exports.producer_task = function (mqUrlArr, queueName, msg, durable, deliveryMode, reconnectTime) {
    amqClient.task(mqUrlArr, queueName, msg, durable, deliveryMode, reconnectTime);
};
exports.producer_fanout = function (mqUrlArr, exchangeName, msg, durable, reconnectTime) {
    amqClient.fanout(mqUrlArr, exchangeName, msg, durable, reconnectTime);
};
exports.producer_direct = function (mqUrlArr, exchangeName, msg, durable, severity, reconnectTime) {
    amqClient.direct(mqUrlArr, exchangeName, msg, durable, severity, reconnectTime);
};
exports.producer_topic = function (mqUrlArr, exchangeName, msg, durable, topicKey, reconnectTime) {
    amqClient.topic(mqUrlArr, exchangeName, msg, durable, topicKey, reconnectTime);
};
exports.producer_send = function (mqUrlArr, queueName, msg, durable, reconnectTime) {
    amqClient.send(mqUrlArr, queueName, msg, durable, reconnectTime);
};