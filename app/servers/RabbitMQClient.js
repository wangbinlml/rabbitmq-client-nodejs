#!/usr/bin/env node

var amqp = require('amqplib');
var when = require('when');
var defer = when.defer;
var uuid = require('node-uuid');

/**
 * 计时器，循环MQ地址
 * @type {number}
 */
var mqIndex = 0;


/**
 * 说明： 构造函数，
 */
function RabbitMQClient() {
}
module.exports = RabbitMQClient;
RabbitMQClient.prototype = {
    rpc: function (mqUrlArr, queueName, msg, exclusive, noAck, reconnectTime, callback) {
        rmq_rpc(mqUrlArr, queueName, msg, exclusive, noAck, reconnectTime, callback);
    },
    task: function (mqUrlArr, queueName, msg, durable, deliveryMode, reconnectTime) {
        rmq_task(mqUrlArr, queueName, msg, durable, deliveryMode, reconnectTime);
    },
    fanout: function (mqUrlArr, exchangeName, msg, durable, reconnectTime) {
        rmq_fanout(mqUrlArr, exchangeName, msg, durable, reconnectTime);
    },
    direct: function (mqUrlArr, exchangeName, msg, durable, severity, reconnectTime) {
        rmq_direct(mqUrlArr, exchangeName, msg, durable, severity, reconnectTime);
    },
    topic: function (mqUrlArr, exchangeName, msg, durable, topicKey, reconnectTime) {
        rmq_topic(mqUrlArr, exchangeName, msg, durable, topicKey, reconnectTime);
    },
    send: function (mqUrlArr, queueName, msg, durable, reconnectTime) {
        rmq_send(mqUrlArr, queueName, msg, durable, reconnectTime);
    }
}

/**
 * rpc模式调用
 * @param mqUrlArr 连接地址数组
 * @param queueName 队列名称
 * @param msg   发送的消息
 * @param exclusive 是否锁定通道，exclusive如果是true，进程会锁定queue通道，
 * @param noAck
 * @param reconnectTime 重新连接时间
 * @param callback  回调消息
 */
function rmq_rpc(mqUrlArr, queueName, msg, exclusive, noAck, reconnectTime, callback) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {
        amqp.connect(url).then(function (conn) {
            return when(conn.createChannel().then(function (ch) {
                var answer = defer();
                var corrId = uuid();

                function maybeAnswer(msg) {
                    if (msg.properties.correlationId === corrId) {
                        answer.resolve(msg.content.toString());
                    }
                }

                //exclusive如果是true，进程会锁定queue通道，
                var ok = ch.assertQueue('', {exclusive: exclusive})
                    .then(function (qok) {
                        return qok.queue;
                    });

                ok = ok.then(function (queue) {
                    return ch.consume(queue, maybeAnswer, {noAck: noAck})
                        .then(function () {
                            return queue;
                        });
                });

                ok = ok.then(function (queue) {
                    ch.sendToQueue(queueName, new Buffer(msg.toString()), {
                        replyTo: queue, correlationId: corrId
                    });
                    return answer.promise;
                });

                return ok.then(callback);
                /**
                 function (fibN) {
                    console.log(' [.] Got ', fibN);
                    }
                 */

            })).ensure(function () {
                conn.close();
            });
        }).then(null, function () {
            mqCount();
            setTimeout(rmq_rpc(mqUrlArr, queueName, msg, exclusive, noAck, reconnectTime, callback), reconnectTime);
            console.log("连接失败，正在重试，",mqUrlArr);
        });
    } catch (e) {
        console.log(e);
    }

    function mqCount() {
        if (mqIndex == (mqUrlArr.length - 1)) {
            mqIndex = 0;
        } else {
            mqIndex++;
        }
    }
}

/**
 * task-work模式调用
 * @param mqUrlArr  连接地址数组
 * @param queueName 队列名称
 * @param msg   发送的消息
 * @param durable   是否持久化
 * @param deliveryMode
 * @param reconnectTime 重新连接时间
 */
function rmq_task(mqUrlArr, queueName, msg, durable, deliveryMode, reconnectTime) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {
        amqp.connect(url).then(function (conn) {
            return when(conn.createChannel().then(function (ch) {
                var ok = ch.assertQueue(queueName, {durable: durable});
                return ok.then(function () {
                    ch.sendToQueue(queueName, new Buffer(msg.toString()), {deliveryMode: deliveryMode});
                    return ch.close();
                });
            })).ensure(function () {
                conn.close();
            });
        }).then(null, function () {
            mqCount();
            setTimeout(rmq_task(mqUrlArr, queueName, msg, durable, deliveryMode, reconnectTime), reconnectTime);
            console.log("连接失败，正在重试，",mqUrlArr);
        });
    } catch (e) {
        console.log(e);
    }

    function mqCount() {
        if (mqIndex == (mqUrlArr.length - 1)) {
            mqIndex = 0;
        } else {
            mqIndex++;
        }
    }
}

/**
 * fanout模式调用
 * @param mqUrlArr  连接地址数组
 * @param exchangeName  交换器名称
 * @param msg   发送的消息
 * @param durable   是否持久化
 * @param reconnectTime 重新连接时间
 */
function rmq_fanout(mqUrlArr, exchangeName, msg, durable, reconnectTime) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {
        amqp.connect(url).then(function (conn) {
            return when(conn.createChannel().then(function (ch) {
                var ok = ch.assertExchange(exchangeName, 'fanout', {durable: durable})

                return ok.then(function () {
                    ch.publish(exchangeName, '', new Buffer(msg));
                    return ch.close();
                });
            })).ensure(function () {
                conn.close();
            });
        }).then(null, function () {
            mqCount();
            setTimeout(rmq_fanout(mqUrlArr, exchangeName, msg, durable, reconnectTime), reconnectTime);
            console.log("连接失败，正在重试，",mqUrlArr);
        });
    } catch (e) {
        console.log(e);
    }

    function mqCount() {
        if (mqIndex == (mqUrlArr.length - 1)) {
            mqIndex = 0;
        } else {
            mqIndex++;
        }
    }
}


/**
 * direct模式调用
 * @param mqUrlArr  连接地址数组
 * @param exchangeName  交换器名称
 * @param msg   发送的消息
 * @param durable   是否持久化
 * @param severity  关键字
 * @param reconnectTime 重新连接时间
 */
function rmq_direct(mqUrlArr, exchangeName, msg, durable, severity, reconnectTime) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {
        amqp.connect(url).then(function (conn) {
            return when(conn.createChannel().then(function (ch) {
                var ok = ch.assertExchange(exchangeName, 'direct', {durable: durable});
                return ok.then(function () {
                    ch.publish(exchangeName, severity, new Buffer(msg));
                    return ch.close();
                });
            })).ensure(function () {
                conn.close();
            });
        }).then(null, function () {
            mqCount();
            setTimeout(rmq_direct(mqUrlArr, exchangeName, msg, durable, severity, reconnectTime), reconnectTime);
            console.log("连接失败，正在重试，",mqUrlArr);
        });
    } catch (e) {
        console.log(e);
    }

    function mqCount() {
        if (mqIndex == (mqUrlArr.length - 1)) {
            mqIndex = 0;
        } else {
            mqIndex++;
        }
    }
}


/**
 * topic模式调用
 * @param mqUrlArr  连接地址数组
 * @param exchangeName  交换器名称
 * @param msg   发送的消息
 * @param durable   是否持久化
 * @param topicKey  关键字
 * @param reconnectTime 重新连接时间
 */
function rmq_topic(mqUrlArr, exchangeName, msg, durable, topicKey, reconnectTime) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {

        amqp.connect(url).then(function (conn) {
            return when(conn.createChannel().then(function (ch) {
                var ok = ch.assertExchange(exchangeName, 'topic', {durable: durable});
                return ok.then(function () {
                    ch.publish(exchangeName, topicKey, new Buffer(msg));
                    return ch.close();
                });
            })).ensure(function () {
                conn.close();
            });
        }).then(null, function () {
            mqCount();
            setTimeout(rmq_topic(mqUrlArr, exchangeName, msg, durable, topicKey, reconnectTime), reconnectTime);
            console.log("连接失败，正在重试，",mqUrlArr);
        });
    } catch (e) {
        console.log(e);
    }

    function mqCount() {
        if (mqIndex == (mqUrlArr.length - 1)) {
            mqIndex = 0;
        } else {
            mqIndex++;
        }
    }
}


/**
 * send模式调用
 * @param mqUrlArr  连接地址数组
 * @param queueName 队列名称
 * @param msg   发送的消息
 * @param durable   是否持久化
 * @param reconnectTime 重新连接时间
 */
function rmq_send(mqUrlArr, queueName, msg, durable, reconnectTime) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {

        amqp.connect(url).then(function (conn) {
            return when(conn.createChannel().then(function (ch) {
                var ok = ch.assertQueue(queueName, {durable: durable});
                return ok.then(function (_qok) {
                    ch.sendToQueue(queueName, new Buffer(msg));
                    return ch.close();
                });
            })).ensure(function () {
                conn.close();
            });
        }).then(null, function () {
            mqCount();
            setTimeout(rmq_send(mqUrlArr, queueName, msg, durable, reconnectTime), reconnectTime);
            console.log("连接失败，正在重试，",mqUrlArr);
        });
    } catch (e) {
        console.log(e);
    }

    function mqCount() {
        if (mqIndex == (mqUrlArr.length - 1)) {
            mqIndex = 0;
        } else {
            mqIndex++;
        }
    }
}


