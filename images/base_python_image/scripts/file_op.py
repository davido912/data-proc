import pandas as pd
from os import environ
from os import listdir
from os.path import join
from sql_gen import TableMD
from psql_client import PgHook
from glob import glob
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def convert_json_to_csv(src_path: str, dst_path: str) -> None:
    df = pd.read_json(path_or_buf=src_path)
    df.to_csv(dst_path + ".csv")


def import_source(tables_md_dir: str) -> None:
    hook = PgHook(
        database="events", user="dev", password="password", host="localhost", port=5432
    )

    for table_md in listdir(tables_md_dir):
        md = TableMD(table_md_path=join(tables_md_dir, table_md))
        logger.debug(f"processing data for table {md.table_name}")
        src_dir_path = join(environ["RAW_DATA_PATH"], md.load_prefix)
        for i, file in enumerate(glob(join(src_dir_path, "**", "*"))):
            src_file_path = join(src_dir_path, file)
            dst_file_path = join(src_dir_path, md.load_prefix + str(i))
            logger.debug(
                f"converting JSON formatted data into CSV: {src_file_path} -> {dst_file_path}"
            )
            convert_json_to_csv(src_path=src_file_path, dst_path=dst_file_path)
            hook.load_to_table(table_md=md, src_path=dst_file_path)
