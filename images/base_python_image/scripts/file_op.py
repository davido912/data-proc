import pandas as pd
from os.path import expandvars
from os import listdir
from os.path import join
from sql_gen import TableMD
from psql_client import PgHook
from glob import glob
from tempfile import TemporaryDirectory
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


#TODO ADD CASES WHEN DATE DOES NOT EXIST AND SO FORTH
def extract_data(
    src_path: str,
    dst_path: str,
    date_filter_key: Optional[str] = None,
    date_filter_val: Optional[str] = None,
) -> None:
    df = pd.read_json(path_or_buf=src_path)
    # TODO: unit test that between only gets the dates on the day we expect
    if date_filter_key and date_filter_val:
        # this will also raise a significant ValueError if format does not correlate
        start_date = datetime.strptime(date_filter_val, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        df = df.loc[df[date_filter_key].between(start_date, end_date)]
    df.to_csv(dst_path, index=False)


class TempDir(TemporaryDirectory):
    # unfortunately TemporaryDirectory hardcodes 0o700 so this is a tweak to the class
    def __enter__(self) -> str:
        os.chmod(self.name, 0o755)
        return self.name


def import_source(
    tables_md_dir: str,
    date_filter_val: Optional[str] = None,
) -> None:
    hook = PgHook(
        database=expandvars("$POSTGRES_DB"),
        user=expandvars("$POSTGRES_USER"),
        password=expandvars("$POSTGRES_PASSWORD"),
        host=expandvars("$POSTGRES_HOST"),
        port=expandvars("$POSTGRES_PORT"),
    )

    for table_md in listdir(tables_md_dir):
        md = TableMD(table_md_path=join(tables_md_dir, table_md))
        logger.debug(f"processing data for table {md.table_name}")
        src_dir_path = join(expandvars("$RAW_DATA_PATH"), md.load_prefix)
        logger.debug(f"src_dir_path is {src_dir_path}")
        with TempDir(dir="/tmp", prefix=md.load_prefix) as tmpdir:
            for i, file in enumerate(glob(join(src_dir_path, "**"))):
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
                hook.load_to_table(table_md=md, src_path=dst_file_path)
