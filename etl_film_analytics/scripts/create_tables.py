import psycopg2

from etl_film_analytics.src.constants import (
    DB_HOST,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)
from etl_film_analytics.src import utils_tables, sql_queries


def main(
        check_content=True
):
    """Create tables in a postgres database
    Note-1: Current tables are dropped
    Note-2: Table names and queries are stored in the `sql_queries` file.
    Args:
        check_content(bool): if True, display tables at the end of execution
    """
    conn = psycopg2.connect(
        f"host={DB_HOST} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )
    utils_tables.reset_tables(conn)
    if check_content:
        utils_tables.check_database_content(sql_queries.table_names, conn)
    conn.close()


if __name__ == "__main__":
    main()
