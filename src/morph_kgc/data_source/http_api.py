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

    # Check if any of the references have a JSONPath filter; it's treated differently.
    # e.g [ rml:predicate sdmx-dimension:ageSemiintervals ; rml:objectMap [ rml:reference "MetaData[?(@.Variable.Id==357)].Nombre" ] ]
    def has_filter(ref: str) -> bool:
        return '[?(' in ref

    simple_refs = [r for r in references if not has_filter(r)]
    filter_refs = [r for r in references if has_filter(r)]

    jsonpath_expression = rml_rule['iterator'] + '.('
    # add top level object of the references to reduce intermediate results (THIS IS NOT STRICTLY NECESSARY)
    for i, reference in enumerate(simple_refs):
        jsonpath_expression += reference.split('.')[0] + ','
        #jsonpath_expression += reference + ','
    jsonpath_expression = jsonpath_expression[:-1] + ')'

    jsonpath_result = JSONPath(jsonpath_expression).parse(json_data)

    # normalize and remove nulls
    json_df = pd.json_normalize([json_object for json_object in normalize_hierarchical_data(jsonpath_result) if
                                 None not in json_object.values()])
    
    if filter_refs:
            join_key = simple_refs[0] 
            entries = JSONPath("$.*").parse(json_data)
            lookup_data = {item.get(join_key): item for item in entries if item.get(join_key)}

            for filter_ref in filter_refs:
                column_value = []
                for key_value in json_df[join_key]:
                    match = lookup_data.get(key_value)
                    if match:
                        res = JSONPath(f"$..{filter_ref}").parse(match)
                        column_value.append(res[0] if res else None)
                    else:
                        column_value.append(None)
                json_df[filter_ref] = column_value

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(json_df.columns)))
    json_df[missing_references_in_df] = None
    #json_df.dropna(axis=1, how='any', inplace=True) #This removes everything if threres a null; it should only keep the columns that are in the reference and remove the rest.
    # Drop rows with None values in some columns
    json_df = json_df.dropna(axis=0, how='any', subset=[c for c in references if c in json_df.columns])
    #
    json_df = json_df[[c for c in references if c in json_df.columns]] #Take only the columns that are in the references.

    return json_df
