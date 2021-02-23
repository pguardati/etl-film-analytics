import re
import pickle


def create_hash_table(file):
    """Create a hash table.
    The table has the following format:
        - key: stripped version of a line
        - value: position of that line in the file
    e.g:
        '<title>Wikipedia: Heat</title>': 12340
        '<title>Wikipedia: Heat (1968 film)</title>': 15000

    Args:
        file(TextIOWrapper): pointer to a file on disk

    Returns:
        dict
    """
    hash_table = {}
    while True:
        # read line position and content
        pos = file.tell()
        line = file.readline().strip()
        # terminate at end of file
        if not line:
            break
        # record file position - overwrite duplicates
        hash_table[line] = pos
    return hash_table


def read_document_abstract(file):
    """Read abstract field of a wikipedia document
    Note: the cursor of the file has to point the beginning
    of the abstract field
    """
    lines = []
    while True:
        line = file.readline().strip()
        # the cursor points to the abstract field, start reading
        if "<abstract>" in line:
            lines.append(line)
            if "</abstract>" in line:
                abstract_text = " ".join(lines)
                match = re.search("<abstract>(.*)</abstract>", abstract_text)
                abstract = match.group(1)
                return abstract
        else:
            # the cursor was not pointing to the abstract field
            return None


def read_document(file, document_position):
    """Read a document and extract its features

    Args:
        file(TextIOWrapper): pointer to wikipedia dataset file
        document_position(int): position of a document in the file,
                                expressed as the number of characters from
                                the beginning of the file

    Returns:
        tuple=[str,str]
    """
    # reach document position
    file.seek(document_position)
    # read raw title and url
    title_line = file.readline()
    url_line = file.readline()
    abstract = read_document_abstract(file)
    # extract title and url
    title = re.search("<title>Wikipedia: (.*)</title>", title_line)
    url = re.search("<url>(.*)</url>", url_line)
    # get processed data
    title = title.group(1) if title else None
    url = url.group(1) if url else None
    return title, url, abstract


def get_document_features(file, document, table):
    """Search a document in a wikipedia file and extract its features

    The feature extraction is done with an hash table,
    in particular:
        - document's title is used to query the hash table
        - the table returns the location of the queried string in the original file
        - the file is accessed in O(1) time at the given location
        - the feature is extracted

    Args:
        file(TextIOWrapper): pointer to wikipedia dataset file
        document(tuple): tuple of document keywords with format (title,year)
        table(dict): hash table of the input file

    """
    title, year = document
    # generate title lines, sorted by importance
    queries = []
    document_pattern = "<title>Wikipedia: %s</title>"
    if year:
        queries.append(
            document_pattern % f"{title} ({year} film)"
        )
    queries += [
        document_pattern % f"{title} (film)",
        document_pattern % title
    ]
    # check if the table contains one of the generated lines
    document = [None, None, None]
    for query in queries:
        if query in table:
            # return features of the first matched document (most probable one)
            document_position = table[query]
            document = read_document(file, document_position)
            break
    return document


def get_documents_features(file, documents, table):
    """Extract documents' features from the wikipedia dataset
    """
    documents_features = []
    for document in documents:
        document = get_document_features(file, document, table)
        documents_features.append(document)
    return documents_features


def search_documents_by_hash(
        file,
        documents,
        table,
        **kwargs
):
    """Load wikipedia dataset from path and extract documents' features
    """
    with open(file, 'r') as text_file:
        documents_features = get_documents_features(
            text_file,
            documents,
            table,
        )
    return documents_features


def get_data_from_wikipedia(
        file,
        documents,
        table_filepath,
        **kwargs
):
    """Extract data from the wikipedia dataset"""
    print(f"Loading hash table from {table_filepath}..")
    with open(table_filepath, 'rb') as table_file:
        table = pickle.load(table_file)
    print(f"Querying features for {len(documents)} films from wikipedia..")
    documents_features = search_documents_by_hash(
        file,
        documents,
        table,
    )
    wikipedia_links = [document[1] for document in documents_features]
    wikipedia_abstracts = [document[2] for document in documents_features]
    return wikipedia_links, wikipedia_abstracts
