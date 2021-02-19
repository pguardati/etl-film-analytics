table_names = ["films"]

"""Drop"""
films_table_drop = """
DROP TABLE IF EXISTS films
"""

"""Create"""
films_table_create = ("""
CREATE TABLE IF NOT EXISTS films(
    id bigint primary key,
    title varchar not null,
    budget bigint not null,
    release_year int not null,
    revenue bigint not null,
    vote_average float not null,
    ratio float not null,
    production_companies varchar not null,
    wikipedia_page_link varchar,
    wikipedia_abstract varchar 
);
""")

"""Insert Pandas->SQL"""
films_table_insert = """
insert into films (
    id,
    title,
    budget,
    release_year,
    revenue,
    vote_average,
    ratio,
    production_companies,
    wikipedia_page_link,
    wikipedia_abstract
) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
