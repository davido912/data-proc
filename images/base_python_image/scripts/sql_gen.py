import yaml
from dataclasses import dataclass
from typing import List, Dict, Union


@dataclass
class TableMD:
    """This class is used as an interface between table metadata YAML files and importers
    :param table_md_path: Path to a table metadata YAML file
    :type table_md_path: str
    """
    table_md_path: str

    def __post_init__(self):
        with open(self.table_md_path, "r") as f:
            self.table_md = yaml.load(f, Loader=yaml.FullLoader)

    @property
    def table_name(self) -> str:
        return self.table_md["table_name"]

    @property
    def schema_name(self):
        return self.table_md["schema"]

    @property
    def columns(self) -> List[dict]:
        return self.table_md["columns"]

    @property
    def delimiter(self) -> str:
        return self.table_md["delimiter"]

    @property
    def load_prefix(self) -> str:
        return self.table_md["load_prefix"]

    @property
    def filter_key(self) -> Union[str, None]:
        # get used to return None when not found because this key is not required
        return self.table_md.get("filter_key")

    @property
    def delta_params(self) -> Union[Dict, None]:
        return self.table_md.get("delta_params")

@dataclass
class SQLGenerator:
    """This class uses the table metadata to construct different SQL strings for dropping/creating/copying data
    into tables.
    :param table_md: Parsed YAML file containing table metadata
    :type table_md: TableMD
    """

    table_md: TableMD

    def drop_table(self) -> str:
        return """DROP TABLE IF EXISTS {schema}.{table_name};
        """.format(
            schema=self.table_md.schema_name, table_name=self.table_md.table_name
        )

    def create_table_query(self) -> str:
        sql = "CREATE TABLE IF NOT EXISTS {schema}.{table_name}({columns});"
        columns = []
        for column_md in self.table_md.columns:
            column = f"{column_md['name']} {column_md['type']}"
            if column_md.get("length"):
                column += f"({column_md['length']})"
            columns.append(column)

        return sql.format(
            schema=self.table_md.schema_name,
            table_name=self.table_md.table_name,
            columns=",".join(columns),
        )

    def copy_query(self, src_path: str) -> str:
        return """COPY {schema}.{table_name} ({columns})
        FROM '{src_path}'
        DELIMITER '{delimiter}'
        CSV HEADER;
        """.format(
            schema=self.table_md.schema_name,
            src_path=src_path,
            table_name=self.table_md.table_name,
            delimiter=self.table_md.delimiter,
            columns=",".join([col.get("name") for col in self.table_md.columns]),
        )

    def upsert_on_id(self) -> str:
        """
        Load data from a delta table (containing only a subset of the data) into a master
        table containing the full dataset. Load replaces entries existing on both tables using a specific key.
        """
        return """
        CREATE TABLE IF NOT EXISTS {schema}.{master_table} (LIKE {schema}.{delta_table});
        DELETE FROM {schema}.{master_table} WHERE {delta_key}
        IN (SELECT {delta_key} FROM {schema}.{delta_table});
        INSERT INTO {schema}.{master_table} SELECT * FROM {schema}.{delta_table};
        """.format(
            schema=self.table_md.schema_name,
            master_table=self.table_md.delta_params["master_table"],
            delta_key=self.table_md.delta_params["delta_key"],
            delta_table=self.table_md.table_name,
        )
