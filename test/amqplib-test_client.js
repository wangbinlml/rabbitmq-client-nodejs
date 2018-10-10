
//使用前需保证安装
// npm install amqplib
// npm install when
// npm install node-uuid

var amq = require('../app/servers/RabbitMQClient');
var user = '{"user_id":"123","name":"zhangsan"}';
var msg = '{"operation_type":"1","data":' + user + '}';//
var url = [
    "amqp://172.16.166.167:5672",
    "amqp://web_admin:admin@172.16.166.226:5672",
    "amqp://web_admin:admin@172.16.166.223:5672"];
var queueName = 'qaz';
// amq.prototype.rpc(url,queueName,msg,true,true,1000,function (data) {
//     console.log("callbck======>"+data)
// });
amq.prototype.direct(url, "abc", msg, true, "info", 1000);
console.log('=============');
