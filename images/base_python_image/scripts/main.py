from datetime import datetime
from file_op import import_sources
import pyfiglet
from termcolor import colored
from pathlib import Path
from os.path import join
from psql_client import PgHook
from typing import Union
from os.path import dirname


ROOT_DIR = Path(__file__).parent.absolute()
TABLE_METADATA_DIR = join(ROOT_DIR, "table_metadata")
SQL_MODELLING_DIR = join(ROOT_DIR, "modelling")
RAW_DATA_DIR = join(dirname(ROOT_DIR), "raw_data")


def cli() -> Union[str, None]:
    print(colored(pyfiglet.figlet_format("WELCOME"), "green"))
    print(
        """Choose one of the following: \n
    (1) load all events 
    (2) load specific date from events
    ** Organizations related data is loaded in full in either of the choices
    """
    )
    while True:
        choice = input("Your choice: ")
        if choice in ["1", "2"]:
            break
        print("Invalid selection, please choose again")
    while True:
        if choice == "1":
            print(colored("Loading all data", "green"))
            return
        elif choice == "2":
            date_filter_val = input("Please insert date in YYYY-MM-DD format: ")
            try:
                datetime.strptime(date_filter_val, "%Y-%m-%d")
                print(colored(f"Loading data for date: {date_filter_val}", "green"))
                return date_filter_val
            except ValueError:
                print(
                    "Invalid date format, please enter a date that is in YYYY-MM-DD format"
                )


def main():
    while True:
        val = cli()
        import_sources(
            tables_md_dir=TABLE_METADATA_DIR,
            raw_data_dir=RAW_DATA_DIR,
            date_filter_val=val,
        )

        with open(join(SQL_MODELLING_DIR, "fact_events.sql"), "r") as f:
            sql = f.read()

        pg_hook = PgHook()
        pg_hook.execute(sql)


if __name__ == "__main__":
    main()
