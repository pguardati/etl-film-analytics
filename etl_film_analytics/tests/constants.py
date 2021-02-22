import os
from etl_film_analytics.src.constants import PROJECT_PATH

DIR_TEST_DATA = os.path.join(PROJECT_PATH, 'tests', 'test_data')

TEST_DB_NAME = "analytics_db_test"
TEST_DB_HOST = "localhost"
TEST_DB_USER = "postgres"
TEST_DB_PASSWORD = ""
TEST_DB_URI = f"postgresql://{TEST_DB_USER}@{TEST_DB_HOST}/{TEST_DB_NAME}"
