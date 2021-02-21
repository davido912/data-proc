import pytest
from file_op import import_sources
from os.path import join
from os import mkdir
from psql_client import PgHook
from tempfile import NamedTemporaryFile, TemporaryDirectory
from tests.mocks import get_mock_json, get_mock_table_md_yaml
from datetime import datetime


def setup_module(module):
    print("SETTING UP TEST ENVIRONMENT IN DATABASE")
    hook = PgHook()
    hook.execute("CREATE SCHEMA test;")


def teardown_module(module):
    print("\nTEARING DOWN TEST ENVIRONMENT IN DATABASE")
    hook = PgHook()
    hook.execute("DROP SCHEMA test CASCADE;")


@pytest.mark.integration
def test_import_sources():
    """This function includes the entire mechanism of extracting the data and loading it to the database.
    It then verifies that the data exists and asserts its content.
    TemporaryDirectories and NamedTemporaryFiles are used for simulating the environment. The import sources function
    uses prefix to pick up files for loading, and therefore requires that the raw data is stored in the following
    structure:
    .
    └── raw_data directory
        └── subdir (e.g. organization_data)
            ├── foo.csv
            └── bar.csv

    """
    raw_data_dir = TemporaryDirectory(dir="/tmp", prefix="raw_data")
    test_data_dir = join(raw_data_dir.name, "test")
    mkdir(test_data_dir)
    raw_data_file = NamedTemporaryFile(dir=test_data_dir, prefix="test")
    raw_data_file.write(get_mock_json().encode("utf-8"))
    raw_data_file.flush()

    table_md_yaml = get_mock_table_md_yaml()
    with TemporaryDirectory(dir="/tmp", prefix="table_metadata") as md_dir:
        with NamedTemporaryFile(dir=md_dir) as md:
            md.write(table_md_yaml.encode("utf-8"))
            md.flush()
            import_sources(tables_md_dir=md_dir, raw_data_dir=raw_data_dir.name)

    raw_data_file.close()
    raw_data_dir.cleanup()

    expected = [
        ("foo", "created", datetime(2020, 12, 8, 20, 3, 16, 759617)),
        ("bar", "created", datetime(2014, 12, 8, 20, 3, 16, 759617)),
    ]
    pg_hook = PgHook()
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM test.test_table_delta;")
            assert cur.fetchall() == expected
