import csv

import pandas as pd


def compute_ratio(path_metadata):
    """Compute the budget to revenue ratio

    Args:
        path_metadata(str): path of the film metadata, as a csv file

    Returns:
        list: ratio as a list of tuples [(id0, ratio0),(id1, ratio1)..]
    """
    budget_to_revenue_ratio = []
    with open(path_metadata, newline='') as csvfile:
        # initialise reader and read header
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)
        # get index of budget and revenue fields
        budget_index, revenue_index, id_index = [
            header.index(col) for col in ["budget", "revenue", "id"]]
        # read and compute ratio
        for row in reader:
            # accept rows with correct number of fields
            if len(row) == len(header):
                id = int(row[id_index])
                budget = int(row[budget_index])
                revenue = int(row[revenue_index])
                ratio = budget / revenue if (
                        budget != 0 and revenue != 0) else -1
                budget_to_revenue_ratio.append((id, ratio))
    # sort ratio in descending order
    budget_to_revenue_ratio.sort(key=lambda tuple: -tuple[1])
    return budget_to_revenue_ratio


def get_metadata(path_metadata, list_of_ids):
    """Retrieve metadata of films that belong to a list of film identifiers.

    Args:
        path_metadata(str): path of the film metadata, as a csv file
        list_of_ids(list): list of film identifiers

    Returns:
        list
    """
    rows = []
    with open(path_metadata, newline='') as csvfile:
        # initialise reader and read header
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)
        id_index = header.index("id")
        for row in reader:
            # accept rows with correct number of fields
            if len(row) == len(header):
                # get row of interest
                film_id = int(row[id_index])
                if film_id in list_of_ids:
                    rows.append(row)
    return rows


def decode_list_of_dictionaries(encoded_list):
    """Decode a list of dictionaries

    Args:
        encoded_list(str): list of dictionaries
                           with format [{"name": name1}, {"name": name2}]
                           encoded as a string
    Returns:
        str: decoded string with format "name1,name2"
    """
    decoded_list = eval(encoded_list)
    companies = []
    for elem in decoded_list:
        companies.append(elem["name"])
    return ",".join(companies)


def aggregate_data_sources(
        film_metadata,
        budget_to_revenue_ratio
):
    """Aggregate data from multiple sources into a clean dataframe

    Args:
        film_metadata(list): metadata from films, as a list of list
        budget_to_revenue_ratio(list): budget to revenue ratio,
                                       as a list of tuples

    Returns:
        pd.DataFrame
    """
    # declare data models
    metadata_column_names = [
        'adult',
        'belongs_to_collection',
        'budget',
        'genres',
        'homepage',
        'id',
        'imdb_id',
        'original_language',
        'original_title',
        'overview',
        'popularity',
        'poster_path',
        'production_companies',
        'production_countries',
        'release_date',
        'revenue',
        'runtime',
        'spoken_languages',
        'status',
        'tagline',
        'title',
        'video',
        'vote_average',
        'vote_count'
    ]
    ratio_column_names = ["id", "ratio"]
    # aggregate and clean dataframes
    df_metadata = pd.DataFrame(
        film_metadata, columns=metadata_column_names)
    df_metadata["id"] = df_metadata["id"].astype(int)
    df_metadata["release_year"] = \
        df_metadata["release_date"].str.split("-").str[0]
    df_metadata["production_companies"] = df_metadata[
        "production_companies"].apply(decode_list_of_dictionaries)
    # merge sources
    df_ratio = pd.DataFrame(
        budget_to_revenue_ratio, columns=ratio_column_names)
    df_processed = pd.merge(df_metadata, df_ratio, on="id")
    # polish processed dataset
    columns_of_interest = [
        "title",
        "budget",
        "release_year",
        "revenue",
        "vote_average",
        "ratio",
        "production_companies"
    ]
    df_processed = df_processed[columns_of_interest]
    df_processed = df_processed[df_processed["ratio"] > 0]
    df_processed = df_processed.sort_values(by="ratio", ascending=False)
    return df_processed


def process_metadata(path_metadata, number_of_elements):
    """Process film metadata into a clean dataframe

    Args:
        path_metadata(str): path of film metadata, as a csv file
        number_of_elements(int): from the list of elements with highest budget
            to revenue ratio, extract only this number of elements

    Returns:
        pd.DataFrame

    """
    budget_to_revenue_ratio = compute_ratio(path_metadata)

    # retrieve elements with highest ratio
    budget_to_revenue_ratio = budget_to_revenue_ratio[:number_of_elements]
    top_ratio_ids = [tuple[0] for tuple in budget_to_revenue_ratio]
    film_metadata = get_metadata(path_metadata, list_of_ids=top_ratio_ids)

    df_processed = aggregate_data_sources(
        film_metadata,
        budget_to_revenue_ratio
    )
    return df_processed
