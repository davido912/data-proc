import pytest
from time import sleep
from os.path import expandvars
import pika
from queue_implementation.producer import Producer

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


def teardown_module(module):
    print("\nTEARING DOWN TEST ENVIRONMENT IN RABBITMQ")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_MQ_HOST))
    channel = connection.channel()
    channel.queue_delete(queue=TEST_QUEUE)


def test_producer():
    producer = Producer(host=RABBIT_MQ_HOST, queue=TEST_QUEUE)
    producer.publish_event(msg="this is a test msg")
