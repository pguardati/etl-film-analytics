import os
import unittest

from etl_film_analytics.tests.constants import DIR_TEST_DATA
from etl_film_analytics.src.search_by_line import \
    search_document, search_documents, \
    search_documents_heuristic, select_document_candidates

file = os.path.join(DIR_TEST_DATA, "wikipedia_test_set.xml")
text_lines = """
        <title>Wikipedia: Heat (1995 film)</title>
        <url>https://en.wikipedia.org/wiki/Heat_(1995_film)</url>
        <abstract>| writer = Michael Mann</abstract>
        ..
        <title>Wikipedia: Heather Fargo</title>
        <url>https://en.wikipedia.org/wiki/Heather_Fargo</url>
        <abstract>| order = 54th</abstract>
        ..
        <title>Wikipedia: Toy Story</title>
        <url>https://en.wikipedia.org/wiki/Toy_Story</url>
        <abstract>| screenplay =</abstract>
        ..
        <title>Wikipedia: Deadfall (1968 film)</title>
        <url>https://en.wikipedia.org/wiki/Deadfall_(1993_film)</url>
        <abstract>| music = Jim Fox</abstract>
"""


class TestSearchFunctions(unittest.TestCase):
    def test_search_document(self):
        expected_result = [
            3,
            'Heat (1995 film)',
            'https://en.wikipedia.org/wiki/Heat_(1995_film)'
        ]
        result = search_document(
            text_lines,
            document=("Heat", 1995)
        )
        self.assertEqual(expected_result, result)

    def test_search_documents(self):
        results = search_documents(
            text_lines,
            documents=[
                ("Heat", 1995),
                ("Toy Story", None),
                ("Deadfall", 1968)
            ]
        )
        self.assertEqual(
            [
                [
                    3,
                    'Heat (1995 film)',
                    'https://en.wikipedia.org/wiki/Heat_(1995_film)'
                ],
                [
                    1,
                    'Toy Story',
                    'https://en.wikipedia.org/wiki/Toy_Story'
                ],
                [
                    3,
                    'Deadfall (1968 film)',
                    'https://en.wikipedia.org/wiki/Deadfall_(1993_film)'
                ]
            ],
            results)

    def test_search_documents_by_batch(self):
        documents = [
            ("Heat", 1995),
            ("Toy Story", None),
        ]
        matches_expected = [
            [
                [3,
                 'Heat (1995 film)',
                 'https://en.wikipedia.org/wiki/Heat_(1995_film)'
                 ],
                [1,
                 'Toy Story',
                 'https://en.wikipedia.org/wiki/Toy_Story'
                 ]
            ]
        ]
        matches = search_documents_heuristic(
            file,
            documents,
            lines_per_batch=int(2 * 1e4)
        )
        self.assertEqual(matches_expected, matches)

    def test_selected_document(self):
        documents = [
            ("Heat", 1995),
            ("Toy Story", None)
        ]
        results_per_batch = [
            [
                [
                    3,
                    'Heat (1995 film)',
                    'https://en.wikipedia.org/wiki/Heat_(1995_film)'
                ],
                [
                    1,
                    'Toy Story',
                    'https://en.wikipedia.org/wiki/Toy_Story'
                ]
            ],
            [
                [
                    2,
                    'Heat Lightning (film)',
                    'https://en.wikipedia.org/wiki/Heat_Lightning_(film)'
                ],
                [-1, None]
            ]
        ]
        selected_documents = select_document_candidates(
            documents,
            results_per_batch
        )
        expected_documents = [
            [
                3,
                'Heat (1995 film)',
                'https://en.wikipedia.org/wiki/Heat_(1995_film)'
            ],
            [
                1,
                'Toy Story',
                'https://en.wikipedia.org/wiki/Toy_Story'
            ]
        ]
        self.assertEqual(expected_documents, selected_documents)
