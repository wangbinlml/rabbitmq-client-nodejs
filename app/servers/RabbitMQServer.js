#!/usr/bin/env node

var amqp = require('amqplib');
var all = require('when').all;

/**
 * 计时器，循环MQ地址
 * @type {number}
 */
var mqIndex = 0;

/**
 *
 * @param mqUrlArr 连接地址数组
 * @param queueName     队列名称
 * @param durable   是否持久化
 * @param noAck
 * @param reconnectTime  重新连接时间，单位毫秒
 * @param prefetchNum  消费者预接收数
 * @param callback   消息回调
 */
function connectRMQ_RPC(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {
        amqp.connect(url).then(function (conn) {
            conn.on('close', function () {
                console.error('Lost connection to RMQ.  Reconnecting in ' + reconnectTime + 'ms...');
                mqCount();
                return setTimeout(connectRMQ_RPC(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback), reconnectTime);
            });
            return conn.createChannel().then(function (ch) {

                var ok = ch.assertQueue(queueName, {durable: durable});
                ok = ok.then(function () {
                    //消费者预取消息数
                    ch.prefetch(prefetchNum);
                    return ch.consume(queueName, doWork, {noAck: noAck});
                });
                return ok.then(function () {
                });
                function doWork(msg) {
                    var body = msg.content.toString();
                    var res = callback(msg);
                    ch.sendToQueue(msg.properties.replyTo,
                        new Buffer(res), {correlationId: msg.properties.correlationId});
                    //延迟发送返回信息
                    // setTimeout(function () {
                    if (noAck == false)
                        ch.ack(msg);
                    // }, reconnectTime);
                }
            });
        }).then(null, function () {
            setTimeout(connectRMQ_RPC(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback), reconnectTime);
            mqCount();
            return console.log("连接失败，正在重试，",mqUrlArr);
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
 * 工作者模式
 * @param mqUrlArr  连接地址数组
 * @param queueName  队列名称
 * @param durable   否持久化
 * @param noAck
 * @param reconnectTime 重新连接时间，单位毫秒
 * @param prefetchNum   消费者预接收数
 * @param callback  消息回调
 */
function connectRMQ_worker(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {
        amqp.connect(url).then(function (conn) {
            conn.on('close', function () {
                console.error('Lost connection to RMQ.  Reconnecting in ' + reconnectTime + 'ms...');
                mqCount();
                return setTimeout(connectRMQ_worker(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback), reconnectTime);
            });
            return conn.createChannel().then(function (ch) {

                var ok = ch.assertQueue(queueName, {durable: durable});
                ok = ok.then(function () {
                    //消费者预取消息数
                    ch.prefetch(prefetchNum);
                    ch.consume(queueName, doWork, {noAck: noAck});
                });
                return ok.then(function () {
                });

                function doWork(msg) {
                    var body = msg.content.toString();
                    // setTimeout(function() {
                    callback(msg);
                    if (noAck == false)
                        ch.ack(msg);
                    // }, reconnectTime);
                }
            });
        }).then(null, function () {
            setTimeout(connectRMQ_worker(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback), reconnectTime);
            mqCount();
            return console.log("连接失败，正在重试，",mqUrlArr);
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
 * fanout模式
 * @param mqUrlArr 连接地址数组
 * @param exchangeName  交换器名称
 * @param durable   是否持久化
 * @param noAck
 * @param exclusive 是否锁定通道，exclusive如果是true，进程会锁定queue通道，
 * @param reconnectTime 重新连接时间，单位毫秒
 * @param callback  消息回调
 */
function connectRMQ_fanout(mqUrlArr, exchangeName, durable, noAck, exclusive, reconnectTime, callback) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {

        amqp.connect(url).then(function (conn) {
            conn.on('close', function () {
                console.error('Lost connection to RMQ.  Reconnecting in ' + reconnectTime + 'ms...');
                mqCount();
                return setTimeout(connectRMQ_fanout(mqUrlArr, exchangeName, durable, noAck, exclusive, reconnectTime, callback), reconnectTime);
            });
            return conn.createChannel().then(function (ch) {
                var ok = ch.assertExchange(exchangeName, 'fanout', {durable: durable});
                ok = ok.then(function () {
                    return ch.assertQueue('', {exclusive: exclusive});
                });
                ok = ok.then(function (qok) {
                    return ch.bindQueue(qok.queue, exchangeName, '').then(function () {
                        return qok.queue;
                    });
                });
                ok = ok.then(function (queue) {
                    return ch.consume(queue, callback, {noAck: noAck});
                });
                return ok.then(function () {
                });
                //替换回调函数callback
                function logMessage(msg) {
                }
            });
        }).then(null, function () {
            setTimeout(connectRMQ_fanout(mqUrlArr, exchangeName, durable, noAck, exclusive, reconnectTime, callback), reconnectTime);
            mqCount();
            return console.log("连接失败，正在重试，",mqUrlArr);
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
 * direct模式
 * @param mqUrlArr mqUrlArr 连接地址数组
 * @param exchangeName 交换器名称
 * @param severities    关键字
 * @param durable   是否持久化
 * @param noAck
 * @param exclusive 是否锁定通道，exclusive如果是true，进程会锁定queue通道，
 * @param reconnectTime 重新连接时间，单位毫秒
 * @param callback  消息回调
 */
function connectRMQ_direct(mqUrlArr, exchangeName, severities, durable, noAck, exclusive, reconnectTime, callback) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {

        amqp.connect(url).then(function (conn) {
            conn.on('close', function () {
                console.error('Lost connection to RMQ.  Reconnecting in ' + reconnectTime + 'ms...');
                mqCount();
                return setTimeout(connectRMQ_direct(mqUrlArr, exchangeName, severities, durable, noAck, exclusive, reconnectTime, callback), reconnectTime);
            });

            return conn.createChannel().then(function (ch) {

                var ok = ch.assertExchange(exchangeName, 'direct', {durable: durable});

                ok = ok.then(function () {
                    return ch.assertQueue('', {exclusive: exclusive});
                });

                ok = ok.then(function (qok) {
                    var queue = qok.queue;
                    var rk = severities;
                    if (severities instanceof Array) {
                    } else {
                        rk = [severities];
                    }
                    return all(rk.map(function (sev) {
                        ch.bindQueue(queue, exchangeName, sev);
                    })).then(function () {
                        return queue;
                    });
                });

                ok = ok.then(function (queue) {
                    return ch.consume(queue, callback, {noAck: noAck});
                });
                return ok.then(function () {
                });

                function logMessage(msg) {
                }
            });
        }).then(null, function () {
            setTimeout(connectRMQ_direct(mqUrlArr, exchangeName, severities, durable, noAck, exclusive, reconnectTime, callback), reconnectTime);
            mqCount();
            return console.log("连接失败，正在重试，",mqUrlArr);
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
 * topic模式
 * @param mqUrlArr  连接地址数组
 * @param exchangeName  交换器名称
 * @param topicKey  关键字
 * @param durable   是否持久化
 * @param noAck
 * @param exclusive 是否锁定通道，exclusive如果是true，进程会锁定queue通道，
 * @param reconnectTime 重新连接时间，单位毫秒
 * @param callback  消息回调
 */
function connectRMQ_topic(mqUrlArr, exchangeName, topicKey, durable, noAck, exclusive, reconnectTime, callback) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {

        amqp.connect(url).then(function (conn) {
            conn.on('close', function () {
                console.error('Lost connection to RMQ.  Reconnecting in ' + reconnectTime + 'ms...');
                mqCount();
                return setTimeout(connectRMQ_topic(mqUrlArr, exchangeName, topicKey, durable, noAck, exclusive, reconnectTime, callback), reconnectTime);
            });

            return conn.createChannel().then(function (ch) {
                var ok = ch.assertExchange(exchangeName, 'topic', {durable: durable});

                ok = ok.then(function () {
                    return ch.assertQueue('', {exclusive: exclusive});
                });

                ok = ok.then(function (qok) {
                    var queue = qok.queue;
                    // var keys = [topicKey];
                    // var keys = ['topic_key.#'];
                    var tk = topicKey;
                    if (topicKey instanceof Array) {
                        url = mqUrlArr[mqIndex];
                    } else {
                        tk = [topicKey];
                    }
                    return all(tk.map(function (rk) {
                        ch.bindQueue(queue, exchangeName, rk);
                    })).then(function () {
                        return queue;
                    });
                });

                ok = ok.then(function (queue) {
                    return ch.consume(queue, callback, {noAck: noAck});
                });
                return ok.then(function () {
                });

                function logMessage(msg) {
                }
            });
        }).then(null, function () {
            setTimeout(connectRMQ_topic(mqUrlArr, exchangeName, topicKey, durable, noAck, exclusive, reconnectTime, callback), reconnectTime);
            mqCount();
            return console.log("连接失败，正在重试，",mqUrlArr);
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
 * send-receive模式
 * @param mqUrlArr  连接地址数组
 * @param queueName 队列名称
 * @param durable   是否持久化
 * @param noAck
 * @param reconnectTime 重新连接时间，单位毫秒
 * @param callback  消息回调
 */
function connectRMQ_receive(mqUrlArr, queueName, durable, noAck, reconnectTime, callback) {
    var url = mqUrlArr;
    if (mqUrlArr instanceof Array) {
        url = mqUrlArr[mqIndex];
    }
    try {
        amqp.connect(url).then(function (conn) {
            conn.on('close', function () {
                console.error('Lost connection to RMQ.  Reconnecting in ' + reconnectTime + 'ms...');
                mqCount();
                return setTimeout(connectRMQ_receive(mqUrlArr, queueName, durable, noAck, reconnectTime, callback), reconnectTime);
            });

            return conn.createChannel().then(function (ch) {
                var ok = ch.assertQueue(queueName, {durable: durable});

                ok = ok.then(function (_qok) {
                    var result = ch.consume(queueName,
                        function (msg) {
                            callback(msg, noAck, ch);
                        },
                        {noAck: noAck});
                    return result;
                });

                return ok.then(function (_consumeOk) {
                });
            });
        }).then(null, function () {
            setTimeout(connectRMQ_receive(mqUrlArr, queueName, durable, noAck, reconnectTime, callback), reconnectTime);
            mqCount();
            return console.log("连接失败，正在重试，",mqUrlArr);
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
 * RPC模式
 */
// connectRMQ_RPC();

/**
 * task-work模式
 */
// connectRMQ_worker();

/**
 * fanout模式
 */
// connectRMQ_fanout();

/**
 * direct模式
 */
// connectRMQ_direct();


/**
 * topic模式
 */
// connectRMQ_topic();

/**
 * send-receive模式
 */
// connectRMQ_receive();

/**
 * 说明： 构造函数，
 */
function RabbitMQServer() {
}
module.exports = RabbitMQServer;
RabbitMQServer.prototype = {
    rpc: function (mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback) {
        connectRMQ_RPC(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback);
    },
    worker: function (mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback) {
        connectRMQ_worker(mqUrlArr, queueName, durable, noAck, reconnectTime, prefetchNum, callback);
    },
    fanout: function (mqUrlArr, exchangeName, durable, noAck, exclusive, reconnectTime, callback) {
        connectRMQ_fanout(mqUrlArr, exchangeName, durable, noAck, exclusive, reconnectTime, callback);
    },
    direct: function (mqUrlArr, exchangeName, severities, durable, noAck, exclusive, reconnectTime, callback) {
        connectRMQ_direct(mqUrlArr, exchangeName, severities, durable, noAck, exclusive, reconnectTime, callback);
    },
    topic: function (mqUrlArr, exchangeName, topicKey, durable, noAck, exclusive, reconnectTime, callback) {
        connectRMQ_topic(mqUrlArr, exchangeName, topicKey, durable, noAck, exclusive, reconnectTime, callback);
    },
    receive: function (mqUrlArr, queueName, durable, noAck, reconnectTime, callback) {
        connectRMQ_receive(mqUrlArr, queueName, durable, noAck, reconnectTime, callback);
    }
}