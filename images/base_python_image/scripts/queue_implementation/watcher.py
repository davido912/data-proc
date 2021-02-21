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

    regexes = [r".*.json$"]
    ignore_directories = True
    case_sensitive = True

    def __init__(self, path: str, watchdog_queue: Queue, rabbitmq_queue: str):
        self.rabbitmq_queue = rabbitmq_queue
        self.path = path
        self.event_handler = RegexMatchingEventHandler(
            regexes=self.regexes,
            ignore_directories=self.ignore_directories,
            case_sensitive=self.case_sensitive,
        )
        self.event_handler.on_created = self.__on_created_event
        self.queue = watchdog_queue
        for file in listdir(path):
            fpath = join(path, file)
            if isfile(fpath):
                event = FileCreatedEvent(fpath)
                self.queue.put(item=event)

        while not watchdog_queue.empty():
            self.__on_created_event(watchdog_queue.get())

    def __on_created_event(self, event):
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

    def start(self):
        observer = Observer()
        observer.schedule(self.event_handler, self.path, recursive=True)
        observer.start()
        observer.join()
