# etl-film-analytics
ETL pipeline that aggregates film data and load them into a PostgreSQL database.  

## Installation

Before to start:  
Add the current project folder path to PYTHONPATH.  
In ~/.bashrc, append:
```
PYTHONPATH=your/path/to/repo:$PYTHONPATH 
export PYTHONPATH
```
e.g.
```
PYTHONPATH=~/PycharmProjects/etl-film-analytics:$PYTHONPATH 
export PYTHONPATH
```

To install and activate the environment:
```
conda env create -f environment.yml
conda activate etl_film_analytics
```



Download data from:
```
https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz (film metadata)
https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7#movies_metadata.csv (wikipedia dataset)
```
and store them in `etl-film-analytics/data`

## Usage
Create a database to store the result of the etl
```
sh etl_film_analytics/scripts/create_database.sh analytics_db
```

To drop the current table and to create an empty one:
```
python etl_film_analytics/scripts/create_tables.py
```

The wikipedia file contains several lines.
In order to search words inside it with reduced time complexity, an hash table has been used.
To generate this hash table, run:
```
python etl_film_analytics/scripts/create_hash_table.py \
--text_filepath=data/enwiki-latest-abstract.xml \
--table_filepath=data/enwiki-latest-abstract-hashtable.pickle
```

To run the etl pipeline on the full data,  
```
python etl_film_analytics/scripts/etl.py --metadata_filepath=... --wikipedia_filepath=...
```
e.g:
```
python etl_film_analytics/scripts/etl.py \
--metadata_filepath=data/movies_metadata.csv \
--wikipedia_filepath=data/enwiki-latest-abstract.xml \
--number_of_elements=1000
```

## Tests
Create a test database:
```
sh etl_film_analytics/scripts/create_database.sh analytics_db_test
```
To run all unittests:
```
python -m unittest discover etl_film_analytics/tests
```
