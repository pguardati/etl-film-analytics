import os
import sys
import time
import argparse

from etl_film_analytics.tests.constants import DIR_TEST_DATA
from etl_film_analytics.src.text_search import \
    search_documents_naive, search_documents_heuristic


def timeit(method):
    """Measure the execution time of the decorated function"""

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print(f'{kw["search_algorithm"].__name__}: {(te - ts) * 1000} ms')
        return result

    return timed


@timeit
def test_search_algorithm(file, search_algorithm):
    """Test time performance of a search algorithm"""
    documents = [
        ("Heat", 1995),
        ("Toy Story", None),
        ("Deadfall", 1968)
    ]
    candidates = search_algorithm(
        file,
        documents,
        lines_per_batch=int(2 * 1e4)
    )
    return candidates


def run(args):
    test_search_algorithm(args.wikipedia_filepath,
                          search_algorithm=search_documents_naive)
    test_search_algorithm(args.wikipedia_filepath,
                          search_algorithm=search_documents_heuristic)


def parse_input(args):
    parser = argparse.ArgumentParser(
        description="Compare time performance of searching methods")
    parser.add_argument(
        "--wikipedia_filepath",
        help="Path where is stored an xml source from wikipedia",
        default=os.path.join(DIR_TEST_DATA, "wikipedia_test_set.xml"))
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)
    run(args)


if __name__ == "__main__":
    main()
