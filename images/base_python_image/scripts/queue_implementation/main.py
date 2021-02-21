import pyfiglet
from termcolor import colored
from queue import Queue
from pathlib import Path
from os.path import join
from psql_client import PgHook
from os.path import expandvars
from sql_gen import TableMD
from sys import argv
from queue_implementation.watcher import Watcher
from queue_implementation.consumer import Consumer


ROOT_DIR = Path(__file__).parent.absolute()
TABLE_METADATA_PATH = join(ROOT_DIR, "table_metadata", "raw_events.yaml")
RAW_DATA_DIR = join(ROOT_DIR, "raw_data")


if __name__ == "__main__":
    if argv[1] == "producer":
        watchdog_queue = Queue()
        watcher = Watcher(
            path=RAW_DATA_DIR, watchdog_queue=watchdog_queue, rabbitmq_queue="events"
        )
        watcher.start()
    elif argv[1] == "consumer":
        consumer = Consumer(
            host=expandvars("$RABBITMQ_HOST"),
            queue="events",
            table_md=TableMD(table_md_path=TABLE_METADATA_PATH),
        )
        consumer.consume_events()

    elif argv[1] == "consumer-batch":
        consumer = Consumer(
            host=expandvars("$RABBITMQ_HOST"),
            queue="events",
            table_md=TableMD(table_md_path=TABLE_METADATA_PATH),
        )
        consumer.batch_load_to_pgres()
