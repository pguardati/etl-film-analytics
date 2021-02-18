import psycopg2
import pandas as pd

from etl_film_analytics.src.constants import (
    DB_HOST,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)
from etl_film_analytics.src import sql_queries
from etl_film_analytics.src import utils_tables


def main():
    print("Connecting to the database..")
    conn = psycopg2.connect(
        f"host={DB_HOST} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )
    cur = conn.cursor()

    print("Extracting data from disk..")
    df = pd.DataFrame([[
        "Toy Story",
        30000000,
        1995 - 10 - 30,
        373554033,
        7.7,
        "Pixar Animated Studio, Apple",
        10.1,
        "https://wikipedia/Toy_Story",
        "In a world where toys are living things but.."
    ]], columns=[
        "title",
        "budget",
        "release_year",
        "revenue",
        "vote_average",
        "production_companies",
        "ratio",
        "wikipedia_page_link",
        "wikipedia_abstract"
    ])

    print("Loading data into the database..")
    for i, row in df.iterrows():
        cur.execute(sql_queries.films_table_insert, row.values)
    conn.commit()

    print("Displaying the destination table:")
    utils_tables.check_database_content(["films"], conn)


if __name__ == "__main__":
    main()
