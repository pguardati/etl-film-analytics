import os
import shutil
import unittest

from etl_film_analytics.scripts import etl, create_tables, create_hash_table
from etl_film_analytics.tests.constants import DIR_TEST_DATA, TEST_DB_URI


class TestEtl(unittest.TestCase):
    def setUp(self):
        create_tables.main(db_uri=TEST_DB_URI)

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
            "--table_filepath={}".format(os.path.join(
                DIR_TEST_DATA, "wikipedia_test_set_hashtable.pickle")
            )
        ])


class TestHashing(unittest.TestCase):
    def setUp(self):
        self.path_generated_test_files = os.path.join(
            DIR_TEST_DATA, "generated")
        os.makedirs(self.path_generated_test_files, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.path_generated_test_files)

    def test_create_hash_table(self):
        create_hash_table.main([
            "--text_filepath={}".format(os.path.join(
                DIR_TEST_DATA, "wikipedia_test_set.xml")),
            "--table_filepath={}".format(
                os.path.join(self.path_generated_test_files,
                             "wikipedia_test_set.pickle"))
        ])


if __name__ == "__main__":
    unittest.main()
