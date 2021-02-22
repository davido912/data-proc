from watchdog.observers import Observer
from queue import Queue
from os.path import join, isfile, sep
from os import listdir
from watchdog.events import RegexMatchingEventHandler, FileCreatedEvent
from queue_implementation.producer import Producer
import json
import shutil
from os.path import expandvars
from pathlib import Path
from termcolor import colored


class Watcher:
    """
    This class uses the Watchdog module at its core to monitor changes in directory. Only .json files are monitored.
    To note, Watchdog does not parse files that have already existed in the target path, therefore a normal queue is used
    to first parse the directory and then monitor any following changes.
    The Watcher triggers the Producer to publish events on RabbitMQ whenever new .json files are added.
    :param path: Host name of the RabbitMQ broker (e.g. localhost/container name)
    :type path: str
    :param watchdog_queue: Queue used by watchdog for parsing files already existing in the monitor directory
    :type watchdog_queue: Queue
    :param rabbitmq_queue: Designated RabbitMQ queue to publish messages onto
    :type rabbitmq_queue: str
    """

    regexes = [r".*.json$"]
    ignore_directories = True
    case_sensitive = True

    def __init__(self, path: str, watchdog_queue: Queue, rabbitmq_queue: str) -> None:
        self.rabbitmq_queue = rabbitmq_queue
        self.path = path
        self.event_handler = RegexMatchingEventHandler(
            regexes=self.regexes,
            ignore_directories=self.ignore_directories,
            case_sensitive=self.case_sensitive,
        )
        self.event_handler.on_created = self.__on_created_event
        self.watchdog_queue = watchdog_queue
        for file in listdir(path):
            fpath = join(path, file)
            if isfile(fpath):
                event = FileCreatedEvent(fpath)
                self.watchdog_queue.put(item=event)

        while not watchdog_queue.empty():
            self.__on_created_event(watchdog_queue.get())

    def __on_created_event(self, event: FileCreatedEvent) -> None:
        print(f"{event.src_path} has been created")
        producer = Producer(
            host=expandvars("$RABBITMQ_HOST"), queue=self.rabbitmq_queue
        )
        with open(event.src_path, "r") as f:
            data = json.loads(f.read())
        if isinstance(data, list):  # if a file containing list of dicts
            for i, entry in enumerate(data):
                producer.publish_event(msg=json.dumps(entry))
            print(
                colored(f"{i + 1} events were produced", "blue")
            )  # I'm taking into account that we don't feed empty files
        elif isinstance(data, dict):
            producer.publish_event(msg=json.dumps(data))
            print("1 event was produced")

        processed_dir = join(
            sep, "tmp", "processed"
        )  # after having processed the file, it is moved
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        shutil.move(src=event.src_path, dst=processed_dir)
        producer.close()

    def start(self) -> None:
        observer = Observer()
        observer.schedule(self.event_handler, self.path, recursive=True)
        observer.start()
        observer.join()
