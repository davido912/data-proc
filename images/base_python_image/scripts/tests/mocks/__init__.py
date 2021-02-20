from tempfile import NamedTemporaryFile
from sql_gen import TableMD


def get_mock_json():
    return """
    [
      {
        "id": "foo",
        "event_type": "created",
        "event_ts": "2020-12-08 20:03:16.759617"
      },
      {
        "id": "bar",
        "event_type": "created",
        "event_ts": "2014-12-08 20:03:16.759617"
      }
    ]
    """


def get_mock_table_md_yaml():
    return """
    table_name: test_table_delta
    load_prefix: test
    schema: test

    delimiter: ","
    filter_key: event_ts

    delta_params:
      master_table: test_table
      delta_key: id

    columns:
      - name: id
        type: varchar
        length: 300
      - name: event_type
        type: varchar
        length: 100
      - name: event_ts
        type: timestamp
    """


def get_mock_table_md():
    mock_yaml = get_mock_table_md_yaml()
    with NamedTemporaryFile() as f:
        f.write(mock_yaml.encode("utf-8"))
        f.seek(0)
        md = TableMD(f.name)
    return md

