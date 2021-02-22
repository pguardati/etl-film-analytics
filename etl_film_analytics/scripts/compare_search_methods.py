import argparse
import os
import sys
import time
import pickle

from etl_film_analytics.src.search_by_line import \
    search_documents_naive, search_documents_heuristic
from etl_film_analytics.src.search_by_hash import create_hash_table, \
    search_documents_by_hash
from etl_film_analytics.tests.constants import DIR_TEST_DATA


def timeit(method):
    """Measure the execution time of the decorated function"""

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        algorithm = kw["search_algorithm"].__name__
        label = kw["test_label"] if "test_label" in kw else ""
        print(f'{algorithm}{label}: {(te - ts) * 1000} ms')
        return result

    return timed


@timeit
def test_search_algorithm(
        file,
        documents,
        search_algorithm,
        **kwargs
):
    candidates = search_algorithm(
        file=file,
        documents=documents,
        **kwargs
    )
    return candidates


def test_search_algorithm_on_multiple_queries(**kwargs):
    test_search_algorithm(
        **kwargs,
        documents=[
            ("Heat", 1995)
        ],
        test_label="_1_query"
    )
    test_search_algorithm(
        **kwargs,
        documents=[
            ("Heat", 1995),
            ("The Cookout", None),
            ("Jumanji 2", None),
            ("Jumanji", None),
            ("Toy Story", None),
        ],
        test_label="_5_query"
    )
    test_search_algorithm(
        **kwargs,
        documents=[
            ("Heat", 1995),
            ("The Cookout", None),
            ("Jumanji 2", None),
            ("Jumanji", None),
            ("Toy Story", None),
            ("Toy Story 3", None),
            ("Toy Story 2", None),
            ("The Cookout", None),
            ("Deadfall", None),
            ("Never Talk to Strangers", None)
        ],
        test_label="_10_query"
    )


def run(args):
    test_search_algorithm_on_multiple_queries(
        file=args.wikipedia_filepath,
        search_algorithm=search_documents_naive,
        lines_per_batch=int(2 * 1e4)
    )
    test_search_algorithm_on_multiple_queries(
        file=args.wikipedia_filepath,
        search_algorithm=search_documents_heuristic,
        lines_per_batch=int(2 * 1e4)
    )
    # create hash table and test hash search with preloaded table
    with open(args.wikipedia_filepath, 'r') as file:
        table = create_hash_table(file)
    test_search_algorithm_on_multiple_queries(
        file=args.wikipedia_filepath,
        search_algorithm=search_documents_by_hash,
        table=table
    )


def parse_input(args):
    parser = argparse.ArgumentParser(
        description="Compare time performance of searching methods")
    parser.add_argument(
        "--wikipedia_filepath",
        help="Path where it is stored an xml source from wikipedia",
        default=os.path.join(DIR_TEST_DATA, "wikipedia_test_set.xml"))
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)
    run(args)


if __name__ == "__main__":
    main()
