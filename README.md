# etl-film-analytics
ETL pipeline that aggregates film data and load them into a PostgreSQL database.  

## Design
This pipeline aggregates 2 source of data:
- film metadata (title, year, ..)
- data from wikipedia (subset of wikipedia source, in xml format)
 
The **film metadata** contains 45k films.  
To process it, a **line by line** algorithm has shown sufficient time performance.  
Hence, no additional solution has been investigated.

The **data from wikipedia** contains ~70M lines
and the requirement is to query this file for each of the 45k films.  
A naive approach (divide the file in batches and search the films in each batch) has shown insufficient performance.  
Hence, a solution based on an **hash table** has been used.  
In particular the hash table is created once, scrolling the wikipedia dataset line by line.
Then, at query time, the films are queried in linear time 
since the location of each line has been previously recorded in the hash table.


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

Install and activate the environment:
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

## Tests
Create a test database:
```
sh etl_film_analytics/scripts/create_database.sh analytics_db_test
```
Run unittests:
```
python -m unittest discover etl_film_analytics/tests
```

## Usage
Create a database to store the result of the etl
```
sh etl_film_analytics/scripts/create_database.sh analytics_db
```

Create an empty table in the database:
```
python etl_film_analytics/scripts/create_tables.py
```

The wikipedia file contains several lines (~7Gb).  
In order to search multiple words efficiently, an hash table has been used.
To generate this hash table, run:
```
python etl_film_analytics/scripts/create_hash_table.py \
--text_filepath=data/enwiki-latest-abstract.xml \
--table_filepath=data/enwiki-latest-abstract-hashtable.pickle
```
Note: this action requires ~10' on a Macbook Pro 2015  
output:
```
Creating an Hash table..
This could take several minutes..
Elapsed time: 687.85 s
Storing the Hash table on disk..
Hash table stored in data/enwiki-latest-abstract-hashtable.pickle
```

Once the hash table has been generated,
run the etl pipeline on the full data,  
```
python etl_film_analytics/scripts/etl.py 
--metadata_filepath=.. \
--wikipedia_filepath=.. \
--table_filepath=..
```
e.g:
```
python etl_film_analytics/scripts/etl.py \
--metadata_filepath=data/movies_metadata.csv \
--wikipedia_filepath=data/enwiki-latest-abstract.xml \
--table_filepath=data/enwiki-latest-abstract-hashtable.pickle \
--number_of_elements=1000
```
output:
```
Connecting to postgresql://postgres@localhost/analytics_db..
Processing film metadata..
Processed 45430 films
Merging metadata with film data from Wikipedia:
Loading hash table from /Users/pietroguardati/PycharmProjects/etl-film-analytics/data/enwiki-latest-abstract-hashtable.pickle..
Querying features for 45430 films from wikipedia..
Merging completed
Selecting films to load..
Loading 1000 data into the database..
Displaying the destination table:
films
+----+-------+-------------------------+----------+----------------+-----------+----------------+------------------+-----------------------------------------------------------------------------------------+-------------------------------------------------------+----------------------------------------------+
|    |    id | title                   |   budget |   release_year |   revenue |   vote_average |            ratio | production_companies                                                                    | wikipedia_page_link                                   | wikipedia_abstract                           |
|----+-------+-------------------------+----------+----------------+-----------+----------------+------------------+-----------------------------------------------------------------------------------------+-------------------------------------------------------+----------------------------------------------|
|  0 | 14844 | Chasing Liberty         | 23000000 |           2004 |        12 |            6.1 |      1.91667e+06 | Alcon Entertainment,ETIC Films,C.R.G. International,Trademark Films,Micro Fusion 2003-2 | https://en.wikipedia.org/wiki/Chasing_Liberty         | | producer       =                           |
|  1 | 18475 | The Cookout             | 16000000 |           2004 |        12 |            4.6 |      1.33333e+06 | Cookout Productions                                                                     | https://en.wikipedia.org/wiki/The_Cookout             | | runtime        = 97 minutes                |
|  2 | 48781 | Never Talk to Strangers |  6400000 |           1995 |         6 |            4.7 |      1.06667e+06 | TriStar Pictures                                                                        | https://en.wikipedia.org/wiki/Never_Talk_to_Strangers | | music          = Pino DonaggioSteve Sexton |
|  3 | 38140 | To Rob a Thief          |  4002313 |           2007 |         6 |            6   | 667052           | Narrow Bridge Films                                                                     | https://en.wikipedia.org/wiki/To_Rob_a_Thief          | | runtime        = 98 minutes                |
|  4 | 33927 | Deadfall                | 10000000 |           1993 |        18 |            3.1 | 555556           | Trimark Pictures                                                                        | https://en.wikipedia.org/wiki/Deadfall_(1993_film)    | | music          = Jim Fox                   |
+----+-------+-------------------------+----------+----------------+-----------+----------------+------------------+-----------------------------------------------------------------------------------------+-------------------------------------------------------+----------------------------------------------+
Elapsed time: 97.9 s
```

