import psycopg2
import psycopg2.extras
from contextlib import closing
from typing import List
from dataclasses import dataclass
import logging

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
    :type port: int
    """

    database: str
    user: str
    password: str
    host: str
    port: int

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

    def execute(self, query: str) -> None:
        # TODO: add functionality for logging here and also logging if any rows are updated
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
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


hook = PgHook(
    database="voucher", user="voucher", password="password", host="localhost", port=5432
)
sql = """
insert into testors values ('david')"""
hook.execute(query=sql)
