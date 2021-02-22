import pyfiglet
from termcolor import colored
import random
from shutil import copy


def cli() -> None:
    """
    This is really just to have a nice communication with the application and being able to drop a file in a click
    of a button and seeing that the Watcher does pick up the file movement and the postgres is updated (check counts)
    """
    print(colored(pyfiglet.figlet_format("WELCOME"), "green"))
    print(
        """Press 1 in order to drop a file containing events in the raw data directory: \n
        (1) drop file in raw data dir
    """
    )
    while True:
        choice = input("Your choice: ")
        if choice == "1":
            break
        print("Invalid selection, please choose again")


def exec() -> None:
    while True:
        cli()
        copy(src='/tmp/events_sample.json', dst=f'/raw_data/events_sample_{str(random.getrandbits(50))}.json')


if __name__ == "__main__":
    exec()
