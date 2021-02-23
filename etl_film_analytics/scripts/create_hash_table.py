import os
import sys
import time
import pickle
import argparse

from etl_film_analytics.src.constants import DIR_DATA
from etl_film_analytics.src.search_by_hash import create_hash_table


def run(args):
    print("Creating an Hash table..\n"
          "This could take several minutes..")
    start = time.time()
    text_file = open(args.text_filepath, 'r')
    table = create_hash_table(text_file)
    text_file.close()
    print(f"Elapsed time: {time.time() - start:.2} s")
    print("Storing the Hash table on disk..")
    table_file = open(args.table_filepath, 'wb')
    pickle.dump(table, table_file, protocol=pickle.HIGHEST_PROTOCOL)
    table_file.close()
    print(f"Hash table stored in {args.table_filepath}")


def parse_input(args):
    parser = argparse.ArgumentParser(
        description="Create an hash table from a text file")
    parser.add_argument(
        "--text_filepath",
        help="Path where is stored a text file",
        default=os.path.join(DIR_DATA, "test_set_wikipedia.xml")
    )
    parser.add_argument(
        "--table_filepath",
        help="Path where will be stored the hash table",
        default=os.path.join(DIR_DATA, "test_set_wikipedia_hashed.pickle")
    )
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)
    run(args)


if __name__ == "__main__":
    main()
