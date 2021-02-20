import pytest
from sql_gen import SQLGenerator, TableMD
from tests.mocks import get_mock_table_md


#########################
### TableMD tests
##########
def test_table_md():
    md = get_mock_table_md()
    # note - we could also use Marshmallow to define a schema but this is overkill at the moment
    assert md.table_name == "test_table_delta"
    assert md.schema_name == "test"
    assert md.columns == [
        {"name": "id", "type": "varchar", "length": 300},
        {"name": "event_type", "type": "varchar", "length": 100},
        {"name": "event_ts", "type": "timestamp"},
    ]
    assert md.delimiter == ","
    assert md.load_prefix == "test"
    assert md.filter_key == "event_ts"
    assert md.delta_params == {
        "master_table": "test_table",
        "delta_key": "id",
    }


def test_table_md_path_not_exists():
    with pytest.raises(FileNotFoundError):
        TableMD("test")


#########################
### SQLGenerator tests
##########
def test_drop_table_query():
    md = get_mock_table_md()
    gen = SQLGenerator(md)
    query = gen.drop_table()
    expected = """
    DROP TABLE IF EXISTS test.test_table_delta;
    """
    assert query.strip() == expected.strip()


def test_create_table_query():
    md = get_mock_table_md()
    gen = SQLGenerator(md)
    query = gen.create_table_query()
    expected = """
CREATE TABLE IF NOT EXISTS test.test_table_delta(id varchar(300),event_type varchar(100),event_ts timestamp);
    """
    assert query.strip() == expected.strip()


def test_copy_query():
    md = get_mock_table_md()
    gen = SQLGenerator(md)
    query = gen.copy_query()
    expected = """COPY test.test_table_delta (id,event_type,event_ts) FROM STDIN
    WITH
    DELIMITER ','
    CSV HEADER
    """
    # comparing exact match of string because spaces can cause unexpected assertion failures
    assert query.replace(" ", "") == expected.replace(" ", "")


def test_upsert_on_id_query():
    md = get_mock_table_md()
    gen = SQLGenerator(md)
    query = gen.upsert_on_id()
    expected = """
    CREATE TABLE IF NOT EXISTS test.test_table (LIKE test.test_table_delta);
    DELETE FROM test.test_table WHERE id
    IN (SELECT id FROM test.test_table_delta);
    INSERT INTO test.test_table SELECT * FROM test.test_table_delta;
"""
    # comparing exact match of string because spaces can cause unexpected assertion failures
    assert query.replace(" ", "") == expected.replace(" ", "")
