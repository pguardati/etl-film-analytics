import os

PROJECT_NAME = 'etl_film_analytics'
REPOSITORY_PATH = os.path.realpath(__file__)[
                  :os.path.realpath(__file__).find(PROJECT_NAME)]
PROJECT_PATH = os.path.join(REPOSITORY_PATH, PROJECT_NAME)
DIR_DATA = os.path.join(REPOSITORY_PATH, 'data')

print("project_path:{}".format(PROJECT_PATH))
print("data_path:{}".format(DIR_DATA))

# Database
DB_NAME = "analytics_db"
DB_HOST = "localhost"
DB_USER = "postgres"
DB_PASSWORD = ""
DB_URI = f"postgresql://{DB_USER}@{DB_HOST}/{DB_NAME}"
