# Import libraries
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.analysis import *
import pandas as pd
from whoosh import index as abc
from whoosh.qparser import *
from whoosh import scoring
import utils
from collections import defaultdict
import os


def SearchEngine(queries, text_analyzer, scoring_function, data_path, directory_containing_the_index, top_k):
    # Create a Schema
    schema = Schema(id=ID(stored=True), title=TEXT(stored=False, analyzer=text_analyzer),
                    content=TEXT(stored=False, analyzer=text_analyzer))

    if not os.path.exists(directory_containing_the_index):
        os.mkdir(directory_containing_the_index)

    # Create an empty-Index
    create_in(directory_containing_the_index, schema)

    # Open the Index
    ix = abc.open_dir(directory_containing_the_index)

    # Fill the Index
    writer = ix.writer()

    data = pd.read_csv(data_path, sep=',', header=0)

    for index, row in data.iterrows():
        id = row['ID']
        title = row['Title']
        content = row['Content']

        writer.add_document(id=str(id), title=title, content=content)

    writer.commit()

    ix = abc.open_dir(directory_containing_the_index)

    # Create a MultifieldParser for parsing the input_query
    qp = MultifieldParser(['title', 'content'], ix.schema)

    query_results = dict()
    for index, query in queries.iterrows():
        query_id = query['Query_ID']
        # print(query_id)
        parsed_query = qp.parse(query['Query'])

        # print('Input Query: ' + query['Query'])
        # print('Parsed Query:' + str(parsed_query))

        # Create a Searcher for the Index with the selected Scoring- Function

        docIDs = []

        with ix.searcher(weighting=scoring_function) as searcher:

            # perform a Search
            results = searcher.search(parsed_query, limit=top_k)

            # print the ID of the best document

            for relev in results:
                docIDs.append(relev['id'])
        query_results[str(query_id)] = list(map(int, docIDs))

    # write query results
    utils.write_json('./data/query_results.json', query_results)

    return query_results


def main():
    # text_analyzer = eval('RegexTokenizer() | LowercaseFilter() | StopFilter()')
    # scoring_function = eval('scoring.BM25F()')
    # directory_containing_the_index = './index/'
    data_path = './data/html_content_Cranfield.tsv'
    query_path = './data/cran_Queries.tsv'
    configuration_path = './data/SearchEngines.csv'
    top_k = 30

    ground_truth_cran_path = '.\data\cran_Ground_Truth.tsv'
    ground_truth_cran = utils.read_ground_truth(ground_truth_cran_path)
    print(ground_truth_cran)

    queries = pd.read_csv(query_path, sep='\t')

    configurations = pd.read_csv(configuration_path)
    print(configurations)

    total_query_results = defaultdict(dict)

    for index, SE in configurations.iterrows():
        SE_ID = SE['SE_ID']
        text_analyzer = eval(SE['Text_Analyzer'])
        scoring_function = eval(SE['Scoring_Functions'])
        directory_containing_the_index = './index/' + str(SE_ID)

        query_results = SearchEngine(queries, text_analyzer, scoring_function, data_path, directory_containing_the_index, top_k)
        print(SE_ID, query_results)
        total_query_results[SE_ID] = query_results

    utils.write_json('./data/total_query_results.json', total_query_results)

if __name__ == "__main__":
    main()
