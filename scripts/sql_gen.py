import yaml
from dataclasses import dataclass


@dataclass
class TableMD:
    table_md_path: str

    def __post_init__(self):
        with open(self.table_md_path, "r") as f:
            self.table_md = yaml.load(f, Loader=yaml.FullLoader)

    @property
    def table_name(self):
        return self.table_md["table_name"]

    @property
    def schema_name(self):
        return self.table_md["schema"]

    @property
    def columns(self):
        return self.table_md["columns"]

    @property
    def delimiter(self):
        return self.table_md["delimiter"]


@dataclass
class SQLGenerator:

    table_md: TableMD

    def create_table_query(self) -> str:
        sql = "CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}({columns});"
        columns = []
        for column_md in self.table_md.columns:
            column = f"{column_md['name']} {column_md['type']}"
            if column_md.get("length"):
                column += f"({column_md['length']})"
            columns.append(column)

        return sql.format(
            schema_name=self.table_md.schema_name,
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
