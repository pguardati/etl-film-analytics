import pandas as pd
from tabulate import tabulate

from etl_film_analytics.src import sql_queries


def reset_tables(conn):
    """Drop and create from scratch each table in the database"""
    cur = conn.cursor()
    cur.execute(sql_queries.films_table_drop)
    cur.execute(sql_queries.films_table_create)
    conn.commit()


def check_database_content(table_names, conn):
    """Check top 5 elements for each table"""
    for table in table_names:
        df = pd.read_sql(f"""SELECT * from {table} LIMIT 5""", conn)
        print(table)
        print(tabulate(df, headers=df.columns, tablefmt="psql"))
