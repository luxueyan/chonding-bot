# import pika
# import json

# # 定义连接池
# connection = pika.BlockingConnection(pika.ConnectionParameters(
#     host='127.0.0.1', port=5672, ))
# channel = connection.channel()
# # 声明队列以向其发送消息消息
# channel.queue_declare(queue='chongding')


# def send(data):
#     global channel
#     msg = json.dumps(data)
#     # 注意当未定义exchange时，routing_key需和queue的值保持一致
#     channel.basic_publish(exchange='', routing_key='chongding', body=msg)
#     print('send success msg to rabbitmq')
#     return


# def close():
#     print('close rabbitmq connection')
#     connection.close()  # 关闭连接
#     return
import logging
import json
import pika


class Publisher:
    # EXCHANGE = 'my_exchange'
    # TYPE = 'topic'
    ROUTING_KEY = 'chongding'

    def __init__(self, host):
        logging.basicConfig(level=logging.INFO)
        self._params = pika.connection.ConnectionParameters(
            host=host,)
        # virtual_host=virtual_host,
        # credentials=pika.credentials.PlainCredentials(username, password))
        self._conn = None
        self._channel = None

    def connect(self):
        if not self._conn or self._conn.is_closed:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            # self._channel.exchange_declare(exchange=self.EXCHANGE,
                                           # type=self.TYPE)

    def _publish(self, msg):
        properties = pika.BasicProperties(content_type='application/json')
        self._channel.basic_publish(exchange='',
                                    routing_key=self.ROUTING_KEY,
                                    properties=properties,
                                    body=json.dumps(msg).encode())
        logging.info('message sent: %s', msg)

    def publish(self, msg):
        """Publish msg, reconnecting if necessary."""

        try:
            self._publish(msg)
        except pika.exceptions.ConnectionClosed:
            logging.info('reconnecting to queue')
            self.connect()
            self._publish(msg)

    def close(self):
        if self._conn and self._conn.is_open:
            logging.info('closing queue connection')
            self._conn.close()
