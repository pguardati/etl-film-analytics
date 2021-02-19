import os
import unittest

from etl_film_analytics.src.processing_csv import process_metadata
from etl_film_analytics.tests.constants import DIR_TEST_DATA


class TestProcessingCsv(unittest.TestCase):

    def setUp(self):
        self.path_csv = os.path.join(DIR_TEST_DATA, "metadata_small.csv")

    def test_process_metadata(self):
        """check the top scorer in the test set"""
        df = process_metadata(self.path_csv, number_of_elements=3)
        self.assertEqual(df.iloc[0, :]["title"], "Heat")
