table_names = ["films"]

"""Drop"""
films_table_drop = """
DROP TABLE IF EXISTS films
"""

"""Create"""
films_table_create = ("""
CREATE TABLE IF NOT EXISTS films(
    title varchar primary key,
    budget bigint not null,
    release_year int not null,
    revenue bigint not null,
    vote_average float not null,
    production_companies varchar not null,
    ratio float not null,
    wikipedia_page_link varchar,
    wikipedia_abstract varchar 
);
""")

"""Insert Pandas->SQL"""
films_table_insert = """
insert into films (
    title,
    budget,
    release_year,
    revenue,
    vote_average,
    production_companies,
    ratio,
    wikipedia_page_link,
    wikipedia_abstract
) values (%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
