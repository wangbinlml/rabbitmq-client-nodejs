

//使用前需保证安装
// npm install amqplib
// npm install when
// npm install node-uuid

var amq = require('../servers/RabbitMQServer');
var user = '{"user_id":"123","name":"zhangsan"}';
var msg = '{"operation_type":"1","data":' + user + '}';
var url = [
    "amqp://172.16.166.167:5672",
    "amqp://web_admin:admin@172.16.166.226:5672",
    "amqp://web_admin:admin@172.16.166.223:5672"];
var queueName = 'qaz';

amq.prototype.recive(url, queueName, true, false, 5000, function (res, noAck, ch) {
    console.log("callbck======>" + res.content.toString());
    //业务逻辑
    //消息确认；
    if (!noAck) {
        logger.info("===消息确认");
        ch.ack(res);
    }
})

console.log('=============');
