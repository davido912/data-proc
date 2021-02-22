import pyfiglet
from termcolor import colored
import random
from shutil import copy, move


def cli() -> int:
    """
    This is really just to have a nice communication with the application and being able to drop a file in a click
    of a button and seeing that the Watcher does pick up the file movement and the postgres is updated (check counts)
    """
    print(colored(pyfiglet.figlet_format("WELCOME"), "green"))
    print(
        """Press 1 in order to drop a file containing events in the raw data directory: \n
        (1) drop file in raw data dir
        (2) drop custom file in raw data dir
    """
    )
    while True:
        choice = input("Your choice: ")
        if choice in ["1", "2"]:
            break
        print(colored("Invalid selection, please choose again"), "red")

    while True:
        if choice == "1":
            print(colored("Loading all data from events_sample.json", "green"))
            return 1
        elif choice == "2":
            print(colored("Loading all data from custom_sample.json", "green"))
            return 2


def exec() -> None:
    while True:
        val = cli()
        if val == 1:
            copy(
                src="/tmp/events_sample.json",
                dst=f"/raw_data/events_sample_{str(random.getrandbits(50))}.json",
            )
        if val == 2:
            try:
                move(
                    src="/tmp/custom_sample.json",
                    dst=f"/raw_data/events_custom_{str(random.getrandbits(50))}.json",
                )
            except FileNotFoundError:
                print(
                    colored(
                        "Please ensure you placed a file named custom_sample.json under images/base_python_image/raw_data/events",
                        "red",
                    )
                )


if __name__ == "__main__":
    exec()
