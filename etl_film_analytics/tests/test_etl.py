import os
import unittest

from etl_film_analytics.scripts import etl, create_tables
from etl_film_analytics.tests.constants import DIR_TEST_DATA, TEST_DB_URI


class TestEtl(unittest.TestCase):
    def setUp(self):
        create_tables.main(db_uri=TEST_DB_URI)

    def test_etl_heuristic(self):
        etl.main([
            "--metadata_filepath={}".format(
                os.path.join(DIR_TEST_DATA, "metadata_small.csv")
            ),
            "--wikipedia_filepath={}".format(
                os.path.join(DIR_TEST_DATA, "wikipedia_test_set.xml")
            ),
            "--number_of_elements=10",
            "--number_of_wikipedia_lines=1600",
            "--database_uri={}".format(TEST_DB_URI),
            "--search_algorithm=heuristic"
        ])

    def test_etl_hash_table(self):
        etl.main([
            "--metadata_filepath={}".format(
                os.path.join(DIR_TEST_DATA, "metadata_small.csv")
            ),
            "--wikipedia_filepath={}".format(
                os.path.join(DIR_TEST_DATA, "wikipedia_test_set.xml")
            ),
            "--number_of_elements=10",
            "--database_uri={}".format(TEST_DB_URI),
            "--search_algorithm=hash_table",
            "--table_filepath={}".format(os.path.join(
                DIR_TEST_DATA, "wikipedia_test_set_hashtable.pickle")
            )
        ])


if __name__ == "__main__":
    unittest.main()
