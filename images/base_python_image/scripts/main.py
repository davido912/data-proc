import sys
from file_op import import_source
import pyfiglet
from termcolor import colored
from pathlib import Path
from os.path import join


ROOT_DIR = Path(__file__).parent.absolute()
TABLE_METADATA_DIR = join(ROOT_DIR, "table_metadata")


def main():
    print(colored(pyfiglet.figlet_format("WELCOME"), "green"))
    print(
        """Choose one of the following: \n
    (1) load all events
    (2) load specific date from events
    """
    )
    while True:
        choice = input("Your choice: ")
        if choice in ["1", "2"]:
            break
        print("Invalid selection, please choose again")
    if choice == "1":
        import_source(tables_md_dir=TABLE_METADATA_DIR)
    else:
        date_filter_val = input("Please insert date in YYYY-MM-DD format: ")
        import_source(tables_md_dir=TABLE_METADATA_DIR, date_filter_val=date_filter_val)


if __name__ == "__main__":
    main()
