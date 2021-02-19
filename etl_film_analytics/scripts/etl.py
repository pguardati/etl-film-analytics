import os
import psycopg2

from etl_film_analytics.src.constants import (
    DB_HOST,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DIR_DATA
)
from etl_film_analytics.src import sql_queries
from etl_film_analytics.src import utils_tables, processing_csv

PATH_METADATA = os.path.join(DIR_DATA, "movies_metadata.csv")


def main():
    print("Connecting to the database..")
    conn = psycopg2.connect(
        f"host={DB_HOST} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )
    cur = conn.cursor()

    print("Processing files from disk..")
    df_metadata = processing_csv.process_metadata(path_metadata=PATH_METADATA)
    # mockup insertion of wikipedia data
    df_metadata["wikipedia_abstract"] = None
    df_metadata["wikipedia_page_link"] = None

    print("Loading data into the database..")
    for i, row in df_metadata.iterrows():
        cur.execute(sql_queries.films_table_insert, row.values)
    conn.commit()

    print("Displaying the destination table:")
    utils_tables.check_database_content(["films"], conn)


if __name__ == "__main__":
    main()
