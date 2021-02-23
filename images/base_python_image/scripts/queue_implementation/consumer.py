import pika
import json
from sql_gen import TableMD, SQLGenerator
from psql_client import PgHook


class Consumer:
    """
    This consumer can consume events in two ways:
    (a) Consume events one by one, using a table metadata YAML to extract fields from each message and accordingly
    generating the appropriate SQL to ingest the data
    (b) Consume events in batches of 5, where after every 5 events consumes, they're loaded to the database.
    If there are less than 5 messages left in the queue, it is left to the inactivity timeout to kick
    in and initiate the loading process
    :param host: Host name of the RabbitMQ broker (e.g. localhost/container name)
    :type host: str
    :param queue: RabbitMQ queue to publish messages onto
    :type queue: str
    :param table_md: Table metadata YAML used to create the table and extract relevant fields from the events
    :type table_md: TableMD
    """
    # I chose to use the default Exchange instead of creating a new one for simplicity

    def __init__(self, host: str, queue: str, table_md: TableMD):
        self.host = host
        self.queue = queue
        self.table_md = table_md
        self.fields = [col.get("name") for col in self.table_md.columns]
        self.connection, self.channel = self.__get_conn()

    def __get_conn(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        channel = connection.channel()
        channel.queue_declare(queue=self.queue)
        return connection, channel

    def __load_to_pgres_callback(self, ch, method, properties, body):
        data = json.loads(body)
        # encapsulating values inside single quotes for loading into the database
        row = [data[field] for field in self.fields]
        hook = PgHook()
        sql_gen = SQLGenerator(self.table_md)
        queries = [
            sql_gen.create_table_query(),  # create table if not exists for loading
            sql_gen.insert_values_into(values=row),
        ]
        hook.execute(queries)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def batch_load_to_pgres(self):
        """
        Inactivity timeout is added in cases where there are less than 5 messages left in the queue. If no further messages
        arrive in 15 seconds, the inactivity timeout kicks in and triggers the processing of the batch currently
        stored in memory.
        """
        sql_gen = SQLGenerator(self.table_md)
        hook = PgHook()
        hook.execute(sql_gen.create_table_query())

        while True:
            try:
                connection, channel = self.__get_conn()
                batch = []
                # Get five messages and break out.
                for method_frame, properties, body in channel.consume(
                    queue=self.queue, inactivity_timeout=15
                ):

                    # if no more messages exist in the queue, break out of the loop
                    if not method_frame:
                        break
                    data = json.loads(body)
                    row = [data[field] for field in self.fields]
                    batch.append(sql_gen.insert_values_into(values=row))
                    channel.basic_ack(method_frame.delivery_tag)

                    if method_frame.delivery_tag == 5:
                        break
                # Requeing the rest of the messages after having pulled a batch
                channel.cancel()
                print("processing batch")
                hook.execute(batch)

            # Close the channel and the connection safely when interrupting so we don't get hanging connections
            except KeyboardInterrupt:  # safely
                channel.close()
                connection.close()
                raise

    def consume_events(self):
        self.channel.basic_consume(
            queue=self.queue,
            auto_ack=False,
            on_message_callback=self.__load_to_pgres_callback,
        )
        self.channel.start_consuming()

    def close(self):
        self.connection.close()
