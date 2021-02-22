import pytest
from time import sleep
import random
from os.path import join
import threading
from tempfile import TemporaryDirectory
from os.path import expandvars
import pika
from queue import Queue
from tests.mocks import get_mock_json
from queue_implementation.watcher import Watcher

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


def test_watcher_parses_existing_files():
    raw_data_dir = TemporaryDirectory(dir="/tmp", prefix="raw_data")
    with open(
        join(raw_data_dir.name, f"events_data_{str(random.getrandbits(50))}.json"), "w"
    ) as f:
        f.write(get_mock_json())
        f.flush()
    queue = Queue()
    Watcher(path=raw_data_dir.name, watchdog_queue=queue, rabbitmq_queue=TEST_QUEUE)
    raw_data_dir.cleanup()


def test_watcher_detects_new_file():
    raw_data_dir = TemporaryDirectory(dir="/tmp", prefix="raw_data")
    queue = Queue()
    watcher = Watcher(
        path=raw_data_dir.name, watchdog_queue=queue, rabbitmq_queue=TEST_QUEUE
    )
    watcher_thread = threading.Thread(target=watcher.start, daemon=True)
    watcher_thread.start()

    with open(
        join(raw_data_dir.name, f"events_data_{str(random.getrandbits(50))}.json"), "w"
    ) as f:
        f.write(get_mock_json())
        f.flush()

    sleep(5)  # wait for the watcher to parse the file

    watcher_thread._running = False
    raw_data_dir.cleanup()
