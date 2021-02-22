import os
import sys
import argparse
import psycopg2

from etl_film_analytics.src.constants import (
    DB_HOST,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DIR_DATA
)
from etl_film_analytics.src import sql_queries
from etl_film_analytics.src import utils_tables, processing_csv, text_search


def run(args):
    print("Connecting to the database..")
    conn = psycopg2.connect(
        f"host={DB_HOST} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )
    cur = conn.cursor()

    print("Processing film metadata..")
    df_metadata = processing_csv.process_metadata(
        path_metadata=args.metadata_filepath,
        number_of_elements=args.number_of_elements
    )
    print("Enriching metadata with data from Wikipedia:")
    film_data = df_metadata.loc[:, ["title", "release_year"]].values
    wikipedia_links, wikipedia_abstracts = text_search.get_data_from_wikipedia(
        file=args.wikipedia_filepath,
        documents=film_data
    )
    df_metadata["wikipedia_page_link"] = wikipedia_links
    df_metadata["wikipedia_abstract"] = wikipedia_abstracts

    print("Loading data into the database..")
    for i, row in df_metadata.iterrows():
        cur.execute(sql_queries.films_table_insert, row.values)
    conn.commit()

    print("Displaying the destination table:")
    utils_tables.check_database_content(["films"], conn)


def parse_input(args):
    parser = argparse.ArgumentParser(
        description="Aggregate film data and load them in a database")
    parser.add_argument(
        "--metadata_filepath",
        help="Path where is stored the film metadata",
        default=os.path.join(DIR_DATA, "movies_metadata.csv"))
    parser.add_argument(
        "--wikipedia_filepath",
        help="Path where is stored an xml source from wikipedia",
        default=os.path.join(DIR_DATA, "enwiki-latest-abstract.xml"))
    parser.add_argument(
        "--number_of_elements",
        help="Number of elements to be loaded in the database",
        default=1000, type=int
    )
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)
    run(args)


if __name__ == "__main__":
    main(["--number_of_elements=10"])
