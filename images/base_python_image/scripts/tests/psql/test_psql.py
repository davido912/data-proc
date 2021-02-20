import pytest
from unittest.mock import MagicMock
from psql_client import PgHook
from sql_gen import SQLGenerator
from tempfile import NamedTemporaryFile
from os.path import expandvars

MOCK_DB_AUTH = {
    "database": "test",
    "user": "test",
    "password": "test",
    "host": "test",
    "port": "test",
}


def setup_module(module):
    print("SETTING UP TEST ENVIRONMENT IN DATABASE")
    hook = pgres_hook()
    hook.execute("CREATE SCHEMA test;")


def teardown_module(module):
    print("\nTEARING DOWN TEST ENVIRONMENT IN DATABASE")
    hook = pgres_hook()
    hook.execute("DROP SCHEMA test CASCADE;")


def pgres_hook():
    return PgHook(
        database=expandvars("$POSTGRES_DB"),
        user=expandvars("$POSTGRES_USER"),
        password=expandvars("$POSTGRES_PASSWORD"),
        host=expandvars("$POSTGRES_HOST"),
        port=expandvars("$POSTGRES_PORT"),
    )


def table_metadata_mock():
    """Returns a mock version of a parsed YAML according to the structure defined in TableMD"""
    table_md_mock = MagicMock()
    table_md_mock.table_name = "test_table"
    table_md_mock.schema_name = "test"
    table_md_mock.columns = [
        {"name": "name", "type": "varchar", "length": 300},
        {"name": "id", "type": "varchar"},
    ]
    table_md_mock.delimiter = ","
    table_md_mock.load_prefix = "test"
    table_md_mock.delta_params = None
    return table_md_mock


def test_get_conn(mocker):
    mock = MagicMock()
    mocker.patch("psql_client.psycopg2.connect", mock)
    hook = PgHook(**MOCK_DB_AUTH)
    hook.get_conn()
    mock.assert_called_once_with(
        database="test", user="test", password="test", host="test", port="test"
    )


@pytest.mark.parametrize(
    "queries", ["SELECT * FROM test;", ["SELECT * FROM test;", "SELECT * FROM test2;"]]
)
def test_execute_accepts_types(mocker, queries):
    """Tests that .execute takes both a single string and a list of strings"""
    mock = MagicMock()
    mocker.patch("psql_client.psycopg2.connect", mock)
    hook = PgHook(**MOCK_DB_AUTH)
    hook.execute(queries=queries)


@pytest.mark.parametrize("queries", ["SELECT 1;", ["SELECT 1;", "SELECT 1;"]])
def test_execute(queries):
    hook = pgres_hook()
    hook.execute(queries)


@pytest.fixture
def drop_table():
    """
    a cleanup fixture when needed after a specific test for dropping a designated table
    """
    yield
    table_md_mock = table_metadata_mock()
    hook = pgres_hook()
    hook.execute(f"DROP TABLE {table_md_mock.schema_name}.{table_md_mock.table_name};")


def test_load_to_table(drop_table):
    """Tests that data is loaded to a certain table"""
    hook = pgres_hook()
    table_md_mock = table_metadata_mock()
    header = ["name", "id"]
    row = ["david", "fz234kal"]
    input_data = [header, row]
    with NamedTemporaryFile(
        dir="/tmp", prefix=table_md_mock.load_prefix, mode="w+"
    ) as f:
        for row in input_data:
            f.write(",".join(row) + "\n")
            f.flush()
        hook.load_to_table(src_path=f.name, table_md=table_md_mock)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {table_md_mock.schema_name}.{table_md_mock.table_name};"
            )
            assert list(cur.fetchall()[0]) == row


def test_load_to_table_delta():
    """Tests that upserts work. This is done by adding the delta_params inside the table metadatabase and comparing
    that the row exists in both tables (master and delta) and that indeed the row matches the expected input
    """
    hook = pgres_hook()
    table_md_mock = table_metadata_mock()
    table_md_mock.delta_params = {
        "master_table": table_md_mock.table_name,
        "delta_key": "id",
    }
    table_md_mock.table_name = f"{table_md_mock.table_name}_delta"
    header = ["name", "id"]
    row = ["david", "fz234kal"]
    input_data = [header, row]
    with NamedTemporaryFile(
        dir="/tmp", prefix=table_md_mock.load_prefix, mode="w+"
    ) as f:
        for row in input_data:
            f.write(",".join(row) + "\n")
            f.flush()
        hook.load_to_table(src_path=f.name, table_md=table_md_mock)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {table_md_mock.schema_name}.{table_md_mock.table_name} "
                f"UNION ALL "
                f"SELECT * FROM {table_md_mock.schema_name}.{table_md_mock.delta_params['master_table']}"
            )
            result = cur.fetchall()
            # check that the row was indeed loaded
            assert list(set(result)) == row
            # check that the same row is present both on the delta and master table
            assert result[0] == result[1]


def test_copy_expert():
    hook = pgres_hook()
    table_md_mock = table_metadata_mock()
    sql_gen = SQLGenerator(table_md=table_md_mock)
    header = ["name", "id"]
    row = ["sarah", "fz234kal"]
    input_data = [header, row]

    with NamedTemporaryFile(
        dir="/tmp", prefix=table_md_mock.load_prefix, mode="w+"
    ) as f:
        for row in input_data:
            f.write(",".join(row) + "\n")
            f.flush()
        hook.copy_expert(src_path=f.name, query=sql_gen.copy_query())

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {table_md_mock.schema_name}.{table_md_mock.table_name} WHERE name='sarah';"
            )
            assert list(cur.fetchall()[0]) == row


def test_copy_expert_file_not_exists():
    """Tests that when a file that does not exist is loaded (or tried to) then an exception is raised"""
    hook = pgres_hook()
    table_md_mock = table_metadata_mock()
    sql_gen = SQLGenerator(table_md=table_md_mock)
    with pytest.raises(FileNotFoundError):
        hook.copy_expert(query=sql_gen.copy_query(), src_path="/tmp/notexists.csv")
