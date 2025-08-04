__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import json
import urllib.request
import pandas as pd

from jsonpath import JSONPath
from io import StringIO
from ..utils import normalize_hierarchical_data


def get_http_api_data(config, rml_rule, references):
    import requests
    http_api_df = pd.read_csv(StringIO(config.get('CONFIGURATION', 'http_api_df')))

    df = http_api_df[http_api_df['source'] == rml_rule['logical_source_value']]
    absolute_path = list(df['absolute_path'])[0]
    payload = {}
    headers = {}
    if 'field_name' in df.columns:
        for i, row in df.iterrows():
            if row['field_name'].lower() in ['authorization', 'accept', 'keyid', 'user-agent']:
                headers[row['field_name']] = row['field_value']
            else:
                payload[row['field_name']] = row['field_value']
    json_data = requests.get(absolute_path, params=payload, headers=headers).json()

    jsonpath_expression = rml_rule['iterator'] + '.('
    # add top level object of the references to reduce intermediate results (THIS IS NOT STRICTLY NECESSARY)
    for reference in references:
        jsonpath_expression += reference.split('.')[0] + ','
    jsonpath_expression = jsonpath_expression[:-1] + ')'

    jsonpath_result = JSONPath(jsonpath_expression).parse(json_data)
    # normalize and remove nulls
    json_df = pd.json_normalize([json_object for json_object in normalize_hierarchical_data(jsonpath_result) if
                                 None not in json_object.values()])

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(json_df.columns)))
    json_df[missing_references_in_df] = None
    json_df.dropna(axis=0, how='any', inplace=True)

    return json_df
