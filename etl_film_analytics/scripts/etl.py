import os
import sys
import time
import argparse
import psycopg2

from etl_film_analytics.src.constants import DB_URI, DIR_DATA
from etl_film_analytics.src import sql_queries
from etl_film_analytics.src import utils_tables, processing_csv
from etl_film_analytics.src.search_by_hash import get_data_from_wikipedia


def run(args):
    start = time.time()
    print(f"Connecting to {args.database_uri}..")
    conn = psycopg2.connect(args.database_uri)
    cur = conn.cursor()

    print("Processing film metadata..")
    df_metadata = processing_csv.process_metadata(
        path_metadata=args.metadata_filepath,
    )
    print(f"Processed {len(df_metadata)} films")

    print("Merging metadata with film data from Wikipedia:")
    film_data = df_metadata.loc[:, ["title", "release_year"]].values
    wikipedia_links, wikipedia_abstracts = get_data_from_wikipedia(
        file=args.wikipedia_filepath,
        documents=film_data,
        table_filepath=args.table_filepath
    )
    df_metadata["wikipedia_page_link"] = wikipedia_links
    df_metadata["wikipedia_abstract"] = wikipedia_abstracts
    print("Merging completed")

    print("Selecting films to load..")
    number_of_elements = args.number_of_elements or len(df_metadata)
    df_metadata = df_metadata.iloc[:number_of_elements, :]

    print(f"Loading {len(df_metadata)} data into the database..")
    for i, row in df_metadata.iterrows():
        cur.execute(sql_queries.films_table_insert, row.values)
    conn.commit()

    print("Displaying the destination table:")
    utils_tables.check_database_content(["films"], conn)

    print(f"Elapsed time: {time.time() - start:.3} s")


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
    parser.add_argument(
        "--database_uri",
        help="URI fo a database",
        default=DB_URI, type=str
    )
    parser.add_argument(
        "--table_filepath",
        help="Path where to load the hash table of the wikipedia dataset",
        default=os.path.join(
            DIR_DATA, "enwiki-latest-abstract-hashtable.pickle")
    )
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)
    run(args)


if __name__ == "__main__":
    main()
