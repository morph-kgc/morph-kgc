import logging

def sparql_to_dataframe(mapping_query_results):
    '''
    Transforms the result from a SPARQL query in rdflib to a DataFrame.

    :param sparql_result_set:
    :return:
    '''

    for result_triples_map in mapping_query_results:
        triples_map_exists = False
        print(result_triples_map, '\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
        for triples_map in triples_map_list:
            triples_map_exists = triples_map_exists or (
                    str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))


