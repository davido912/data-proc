import pandas as pd
from os import listdir
from os.path import join
from sql_gen import TableMD
from psql_client import PgHook
from glob import glob
from tempfile import TemporaryDirectory
import logging
from datetime import datetime, timedelta
from typing import Optional

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def extract_data(
    src_path: str,
    dst_path: str,
    date_filter_key: Optional[str] = None,
    date_filter_val: Optional[str] = None,
) -> None:
    """
    Extract data from JSON input file. This function uses the table metadata to decide
    if to filter extracted data according to specific date key.
    :param src_path: Path leading to input JSON file
    :type src_path: str
    :param dst_path: Path for output file
    :type dst_path: str
    :param date_filter_key: Dimension (column) according to which the data should be filtered
    :type date_filter_key: Optional[str]
    :param date_filter_val: The specific date value to filter against
    :type date_filter_val: Optional[str]
    """
    df = pd.read_json(path_or_buf=src_path)
    if date_filter_key and date_filter_val:
        # ensure that pandas column is in correct datetime format for filtering
        df[date_filter_key] = pd.to_datetime(df[date_filter_key])
        # this will raise a significant ValueError if format does not correlate
        start_date = datetime.strptime(date_filter_val, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        df = df.loc[df[date_filter_key].between(start_date, end_date)]
    df.to_csv(dst_path, index=False)


def import_sources(
    tables_md_dir: str,
    raw_data_dir: str,
    date_filter_val: Optional[str] = None,
) -> None:
    """
    This function iterates over table metadata files in a specific directory path, importing
    all of them and loading them to a PostgreSQL database. Glob is used to load several files (if relevant)
    using file prefix. Note that files must end with a .json suffix.
    :param tables_md_dir: Path leading to table metadata directory
    :type tables_md_dir: str
    :param raw_data_dir: Path leading to raw output to extract data from
    :type raw_data_dir: str
    :param date_filter_val: The specific date value to filter against
    :type date_filter_val: Optional[str]
    """
    for table_md in listdir(tables_md_dir):
        md = TableMD(table_md_path=join(tables_md_dir, table_md))
        logger.debug(f"processing data for table {md.table_name}")
        src_dir_path = join(raw_data_dir, md.load_prefix)
        logger.debug(f"src_dir_path is {src_dir_path}")

        with TemporaryDirectory(dir="/tmp", prefix=md.load_prefix) as tmpdir:
            input_data = glob(join(src_dir_path, "*.json"))
            if not input_data:
                logger.info(f"no files were found in {src_dir_path}")

            for i, file in enumerate(input_data):
                src_file_path = join(src_dir_path, file)
                logger.debug(f"src_file_path is {src_file_path}")
                dst_file_path = join(tmpdir, md.load_prefix + str(i))
                logger.debug(
                    f"converting JSON formatted data into CSV: {src_file_path} -> {dst_file_path}"
                )
                extract_data(
                    src_path=src_file_path,
                    dst_path=dst_file_path,
                    date_filter_key=md.filter_key,
                    date_filter_val=date_filter_val,
                )
                pg_hook = PgHook()
                pg_hook.load_to_table(table_md=md, src_path=dst_file_path)
