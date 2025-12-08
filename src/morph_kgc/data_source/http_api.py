__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd
import os
import importlib.util
import sys

from pathlib import Path
from jsonpath import JSONPath
from io import StringIO
from ..utils import normalize_hierarchical_data


def load_module_from_path(module_name, file_path):
    file_path = str(Path(file_path).resolve())
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load spec for {module_name} from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[module_name] = module  # optional: register for reuse
    return module


def get_http_api_data(config, rml_rule, references):
    import requests
    http_api_df = pd.read_csv(StringIO(config.get('CONFIGURATION', 'http_api_df')))

    df = http_api_df[http_api_df['source'] == rml_rule['logical_source_value']]
    absolute_path = list(df['absolute_path'])[0]
    payload = {}
    headers = {}
    if 'field_name' in df.columns:
        for i, row in df.iterrows():
            if row['field_name'] in os.environ:
                field_value = row['field_value'].format(**os.environ)
            else:
                mod = load_module_from_path("dynamic_api_token", config.get_api_token())
                field_value = mod.get_api_token(arg1=row['field_value'])

            if row['field_name'].lower() in ['authorization', 'accept', 'keyid', 'user-agent']:
                headers[row['field_name']] = field_value
            else:
                payload[row['field_name']] = field_value
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
        # Given a filter reference like "MetaData[?(@.Variable.Id==357)].Nombre" we need to convert it to "MetaData[?(@.Variable.Id==357)].(Nombre)"
        last_field = filter_refs[0][filter_refs[0].rfind('.') + 1:] 
        filter = filter_refs[0][:filter_refs[0].rfind('.') + 1] + f"({last_field})"

        # same as above but for filtered references
        jsonpath_result_filters = JSONPath(rml_rule['iterator'] + "." + filter).parse(json_data)
        flat_filters = pd.json_normalize([json_object for json_object in normalize_hierarchical_data(jsonpath_result_filters) if
                                 None not in json_object.values()])
        
        # add the filtered column to the main dataframe
        json_df[filter_refs[0]] = flat_filters

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(json_df.columns)))
    json_df[missing_references_in_df] = None
    #json_df.dropna(axis=1, how='any', inplace=True) #This removes everything if threres a null; it should only keep the columns that are in the reference and remove the rest.
    json_df = json_df[[c for c in references if c in json_df.columns]] #Take only the columns that are in the references.

    return json_df
