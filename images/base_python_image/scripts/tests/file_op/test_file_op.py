from file_op import extract_data, import_sources
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


#########################
### extract_data tests
##########
def test_extract_data():
    expected = [
        "id,event_type,event_ts\n",
        "foo,created,2020-12-08 20:03:16.759617\n",
        "bar,created,2014-12-08 20:03:16.759617\n",
    ]
    with NamedTemporaryFile() as inputfile:
        inputfile.write(get_mock_json().encode("utf-8"))
        inputfile.flush()
        with TemporaryDirectory(dir="/tmp") as tmpdir:
            outputfile = join(tmpdir, "test")
            extract_data(src_path=inputfile.name, dst_path=outputfile)
            with open(outputfile, "r") as f:
                data = f.readlines()
                assert expected == data


def test_extract_data_filter():
    expected = ["id,event_type,event_ts\n", "foo,created,2020-12-08 20:03:16.759617\n"]
    with NamedTemporaryFile() as inputfile:
        inputfile.write(get_mock_json().encode("utf-8"))
        inputfile.flush()
        with TemporaryDirectory(dir="/tmp") as tmpdir:
            outputfile = join(tmpdir, "test")
            extract_data(
                src_path=inputfile.name,
                dst_path=outputfile,
                date_filter_key="event_ts",
                date_filter_val="2020-12-08",
            )
            with open(outputfile, "r") as f:
                data = f.readlines()
                assert expected == data


#########################
### import_sources tests
##########
def test_import_sources():
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
