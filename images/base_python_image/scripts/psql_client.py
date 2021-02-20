import psycopg2
import psycopg2.extras
import psycopg2.extensions
from contextlib import closing
from typing import List
from dataclasses import dataclass
import logging
from sql_gen import SQLGenerator, TableMD
from typing import Optional, Union
from os.path import isfile, expandvars


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclass
class PgHook:
    """
    PostgreSQL hook to connect to a PostgreSQL database and execute commands/load data
    data.
    :param database: database name containing the relevant schemas & tables
    :type database: str
    :param user: username to log in the database
    :type user: str
    :param password: password relevant to the user to log in the database
    :type password: str
    :param host: the endpoint to connect to the database (e.g. localhost)
    :type host: str
    :param port: port exposed by postgres to connect through (default 5432)
    :type port: str
    """

    database: str = expandvars("$POSTGRES_DB")
    user: str = expandvars("$POSTGRES_USER")
    password: str = expandvars("$POSTGRES_PASSWORD")
    host: str = expandvars("$POSTGRES_HOST")
    port: str = expandvars("$POSTGRES_PORT")

    def get_conn(self) -> psycopg2.extensions.connection:
        """
        Connects to a PostgreSQL database and returns a connection object
        :return: A connection object
        :rtype: psycopg2.extensions.connection
        """
        logger.debug(
            f"establishing connection: {{ database: {self.database}, user: {self.user}, host: {self.host}, port: {self.port} }} "
        )
        return psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def execute(self, queries: Union[List[str], str]) -> None:
        """Executes a singular query or several queries against a PostgreSQL database
        :param queries: A single string query or a list of queries to execute
        :type queries: Union[List[str], str]
        """
        if isinstance(queries, str):
            queries = [queries]
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                for query in queries:
                    logger.debug(f"executing query: {query}")
                    cur.execute(query)
            conn.commit()

    def copy_expert(self, query: str, src_path: str) -> None:
        """Load data from a specific CSV file using a COPY command. Note, the psycopg2 given method
        silently fails when file for loading does not exist, hence an exception was added.
        :param query: SQL COPY query
        :type query: str
        :param src_path: Path leading to the file for loading
        :type src_path: str
        """
        if not isfile(src_path):
            raise FileNotFoundError(f"{src_path} not found")

        logger.info(f"copying file from: {src_path}")
        with open(src_path, "r") as f:
            with closing(self.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(query, f)
                conn.commit()

    def load_to_table(
        self,
        src_path: str,
        table_md: Optional[TableMD] = None,
        table_md_path: Optional[str] = None,
    ) -> None:
        """Load data to a designated table using table metadata yaml file to construct the table. Only CSV format
        is valid.
        :param src_path: Path to the file to load into the database (singular file)
        :type src_path: str
        :param table_md: Parsed YAML file containing table metadata
        :type table_md: Optional[TableMD]
        :param table_md_path: Path to a table metadata YAML file
        :type table_md_path: Optional[str]
        """
        if not table_md:
            table_md = TableMD(table_md_path=table_md_path)
        sql_generator = SQLGenerator(table_md=table_md)
        queries = [
            sql_generator.drop_table(),
            sql_generator.create_table_query(),
        ]
        self.execute(queries)

        self.copy_expert(src_path=src_path, query=sql_generator.copy_query())

        if table_md.delta_params:
            self.execute(sql_generator.upsert_on_id())

