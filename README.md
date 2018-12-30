###说明
RabbitMQ Client for Nodejs 
npm install rabbitmq-client-nodejs
###使用
>
	var amq = require('rabbitmq-client-nodejs');
	var url = [
	  "amqp://172.16.166.167:5672",
	  "amqp://web_admin:admin@172.16.166.226:5672",
	  "amqp://web_admin:admin@172.16.166.223:5672"];
	var queueName = 'qaz';
	
	amq.send(url,"queue_name","msg", false, 1000);
