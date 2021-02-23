import unittest
import pickle
import os

from etl_film_analytics.tests.constants import DIR_TEST_DATA
from etl_film_analytics.src.search_by_hash import read_document, \
    get_document_features, get_documents_features


class TestHashSearch(unittest.TestCase):
    def setUp(self):
        text_file = os.path.join(DIR_TEST_DATA,
                                 "wikipedia_test_set.xml")
        table_filepath = os.path.join(DIR_TEST_DATA,
                                      "wikipedia_test_set_hashtable.pickle")
        self.file = open(text_file, 'r')
        with open(table_filepath, 'rb') as table_file:
            self.table = pickle.load(table_file)

    def tearDown(self):
        self.file.close()

    def test_read_document(self):
        query = "<title>Wikipedia: Heat (1995 film)</title>"
        document_position = self.table[query]
        title, url, abstract = read_document(self.file, document_position)
        self.assertEqual('https://en.wikipedia.org/wiki/Heat_(1995_film)', url)

    def test_search_document(self):
        document = ("Heat", 1995)
        title, url, abstract = get_document_features(self.file, document, self.table)
        self.assertEqual('Heat (1995 film)', title)

    def test_search_documents(self):
        documents = [
            ("Heat", 1995),
            ("Toy Story", None),
            ("Deadfall", 1968)
        ]
        documents_features = get_documents_features(
            self.file, documents, self.table)
        self.assertTrue('Heat (1995 film)', documents_features[0][0])


if __name__ == "__main__":
    unittest.main()
