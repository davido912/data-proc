import psycopg2
import psycopg2.extras
from contextlib import closing
from typing import List
from dataclasses import dataclass
import logging
from sql_gen import SQLGenerator, TableMD
from typing import Optional, Union

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclass
class PgHook:
    """
    This hook is used externally to Airflow to connect the API to the Postgres Database containing voucher related
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

    database: str
    user: str
    password: str
    host: str
    port: str

    # def __post_init__(self):

    def get_conn(self):
        """
        This method returns a connection object for postgres.
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
        # TODO: add functionality for logging here and also logging if any rows are updated
        if isinstance(queries, str):
            queries = [queries]
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                for query in queries:
                    logger.debug(f"executing query: {query}")
                    cur.execute(query)
            conn.commit()

    def fetch_query_results(self, query: str) -> List[List[psycopg2.extras.DictRow]]:
        """
        This method executes a query and fetches all of the results in the query (think about yielding)
        :return: A list of lists, where each list contains a psycopg2 DictRow object which allows
                to key -> value retrieve values from it
        :rtype: List[List[psycopg2.extras.DictRow]
        """
        with closing(
            self.get_conn().cursor(cursor_factory=psycopg2.extras.DictCursor)
        ) as cur:
            cur.execute(query)
            return cur.fetchall()

    def load_to_table(
        self, src_path: str, table_md: Optional[TableMD] = None, table_md_path: Optional[str] = None
    ) -> None:
        """
        This method loads CSV formatted files to a PostgreSQL database, the table is created if it does not exist
        """
        if not table_md:
            table_md = TableMD(table_md_path=table_md_path)
        sql_generator = SQLGenerator(table_md=table_md)
        queries = [
            sql_generator.drop_table(),
            sql_generator.create_table_query(),
            sql_generator.copy_query(src_path=src_path),
        ]
        if table_md.delta_params:
            queries.append(sql_generator.upsert_on_id())

        self.execute(queries)

