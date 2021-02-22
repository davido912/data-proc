import pika


class Producer:
    """
    Basic RabbitMQ producer to publish messages to a designated queue
    :param host: Host name of the RabbitMQ broker (e.g. localhost/container name)
    :type host: str
    :param queue: RabbitMQ queue to publish messages onto
    :type queue: str
    """
    # I chose to use the default Exchange instead of creating a new one for simplicity
    def __init__(self, host: str, queue: str):
        self.host = host
        self.queue = queue
        self.connection, self.channel = self.__get_conn()

    def __get_conn(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        channel = connection.channel()
        channel.queue_declare(queue=self.queue)
        return connection, channel

    def publish_event(self, msg: str):
        self.channel.basic_publish(exchange="", routing_key=self.queue, body=msg)

    def close(self):
        self.connection.close()
