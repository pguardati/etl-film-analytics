import re
import tqdm
from itertools import islice


def search_document(text, document):
    """Search a document in an Wikipedia XML text file.
    Multiple patterns are used to look up a matching title.
    Due to this, a score is assigned to each pattern.
    The more general the pattern,
    the less probable it is to have the correct match,
    the lower the score.

    Args:
        text(str): parsed text file
        document(tuple): document information

    Returns:
        tuple=[int,str,str]: extracted information,
                            formatted as (
                                score --> Pattern score of a title
                                title --> Wikipedia Title
                                url --> Wikipedia URL
                            )
    """
    # generate patterns sorted by importance
    title, year = document
    patterns = []
    document_pattern = "<title>Wikipedia: (%s)</title>(\s|\n)*" \
                       "<url>(.*)</url>(\s|\n)*"
    if year:
        patterns.append(
            (3, document_pattern % f"{title}.*{year}.*film.*")
        )
    patterns += [
        (2, document_pattern % f"{title}.*film.*"),
        (1, document_pattern % title)
    ]
    # retrieve the most important pattern only
    result = [-1, None, None]
    for score, pattern in patterns:
        match = re.search(pattern, text, re.MULTILINE)
        if match:
            result = [
                score,
                match.group(1),
                match.group(3)
            ]
            break
    return result


def search_documents(text, documents):
    """Search multiple documents in a text file.
    Extract information about these documents.

    Args:
        text(str): parsed text file
        documents(list): information about multiple documents

    Returns:
        list: [extracted_document0, extracted_document1, .. ]
    """
    results = []
    for document in documents:
        result = search_document(text, document)
        results.append(result)
    return results


def search_documents_heuristic(
        file,
        documents,
        lines_per_batch=int(1e4),
        total_lines=None
):
    """Search a document in the wikipedia dataset.
    The search is done with an heuristic to improve space-time complexity:
    - Every batch is loaded into memory only once.
    - All the documents are searched in that batch.
    - Only when the batch has been fully queried, the next batch is loaded.

    Args:
        file(str): path of a file
        documents(list): information about the documents to search for
        lines_per_batch(int): number of lines to load in memory for each batch
        total_lines(int): total number of lines in file
                          (optional apriori information to save time,
                          in case of big file)

    Returns:
        list: retrieved information with format:
            [ [doc00,doc01], <-- batch0
              [doc10,doc11], <-- batch1
              .. ]
    """
    # query all the documents in each batch
    results_per_batch = []
    with open(file, 'r') as f:
        line_iterator = iter(lambda: tuple(islice(f, lines_per_batch)), ())
        if total_lines:
            line_iterator = tqdm.tqdm(
                line_iterator,
                total=int(total_lines / lines_per_batch),
                desc="Querying wikipedia dataset"
            )
        for text_lines in line_iterator:
            text = "".join(text_lines)
            results = search_documents(text, documents)
            results_per_batch.append(results)
    return results_per_batch


def search_documents_naive(
        file,
        documents,
        lines_per_batch=int(1e4)
):
    """Search document in the wikipedia dataset.
    The search is done with a naive approach:
    - Split the file by batch and search a document for each batch
    - Repeat for each document

    Args:
        file(str):
        documents():
        lines_per_batch():

    Returns:
        list: retrieved information with format:
            [ [batch0, batch1, ..], <-- document0
              [batch0, batch1, ..], <-- document1
              .. ]
    """
    # query one document at a time
    results = []
    for document in documents:
        results_per_document = search_documents_heuristic(
            file,
            documents=[document],
            lines_per_batch=lines_per_batch
        )
        results.append(results_per_document)
    return results


def select_document_candidates(documents, results_per_batch):
    """Select one document among a set of candidates

    Args:
        documents(list): document information
        results_per_batch(list): query result of searched document,
                                 formatted by batch
                                 (check `search_documents_heuristic`)

    Returns:
        list: list of retrieved document information
    """
    # split candidates by document
    document_candidates = [[] for _ in range(len(documents))]
    for batch_result in results_per_batch:
        for i, pattern_result in enumerate(batch_result):
            document_candidates[i].append(pattern_result)
    # select candidate for each document
    selected_documents = []
    for candidates in document_candidates:
        selected_document = sorted(
            candidates, key=lambda x: x[0], reverse=True)[0]
        selected_documents.append(selected_document)
    return selected_documents


def get_data_from_wikipedia(file, documents, total_lines):
    """Extract data from the wikipedia dataset"""
    results_per_batch = search_documents_heuristic(
        file,
        documents,
        lines_per_batch=int(1e6),
        total_lines=total_lines
    )
    select_documents = select_document_candidates(
        documents,
        results_per_batch,
    )
    wikipedia_links = [document[2] for document in select_documents]
    # TODO: this is a mockup of the abstract extraction
    wikipedia_abstracts = [None for _ in range(len(wikipedia_links))]
    return wikipedia_links, wikipedia_abstracts
