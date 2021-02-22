import pytest
from time import sleep
import threading
from os.path import expandvars
import pika
import json
from psql_client import PgHook
from tests.mocks import get_mock_table_md
from tests.mocks import get_mock_json
from queue_implementation.consumer import Consumer
from queue_implementation.producer import Producer
from datetime import datetime


pytestmark = pytest.mark.unittests_rabbitmq

RABBIT_MQ_HOST = expandvars("$RABBITMQ_HOST")
TEST_QUEUE = "test_queue"


def setup_module(module):
    print("SETTING UP TEST QUEUE IN RABBITMQ")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_MQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=TEST_QUEUE)
    # wait for the queue to setup
    sleep(5)
    hook = PgHook()
    hook.execute("CREATE SCHEMA test;")


def teardown_module(module):
    print("\nTEARING DOWN TEST ENVIRONMENT IN DATABASE")
    hook = PgHook()
    hook.execute("DROP SCHEMA test CASCADE;")
    print("\nTEARING DOWN TEST ENVIRONMENT IN RABBITMQ")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_MQ_HOST))
    channel = connection.channel()
    channel.queue_delete(queue=TEST_QUEUE)


@pytest.fixture
def publish_for_consumption():
    producer = Producer(host=RABBIT_MQ_HOST, queue=TEST_QUEUE)
    data = json.loads(get_mock_json())
    for msg in data:
        producer.publish_event(msg=json.dumps(msg))


def test_consumer_consume_events(publish_for_consumption):
    table_md = get_mock_table_md()
    pg_hook = PgHook()
    consumer = Consumer(host=RABBIT_MQ_HOST, queue=TEST_QUEUE, table_md=table_md)
    consumer_thread = threading.Thread(target=consumer.consume_events, daemon=True)
    consumer_thread.start()
    sleep(4)

    expected = [
        ("foo", "created", datetime(2020, 12, 8, 20, 3, 16, 759617)),
        ("bar", "created", datetime(2014, 12, 8, 20, 3, 16, 759617)),
    ]

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_md.schema_name}.{table_md.table_name};")
            assert cur.fetchall() == expected

    consumer_thread.__running = False
