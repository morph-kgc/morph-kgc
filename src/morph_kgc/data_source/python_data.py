__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero", "Miel Vander Sande", "Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import json
import pandas as pd
import numpy as np

from jsonpath import JSONPath
from ..utils import normalize_hierarchical_data


def get_ram_data(rml_rule, references, python_source=None):
    references = list(references)
    source_key = rml_rule['logical_source_value'][1:-1]
    source_value = python_source[source_key]

    if isinstance(source_value, pd.DataFrame):
        for col in source_value.select_dtypes(include=['object']).columns:
            source_value[col] = source_value[col].apply(lambda x:
                                                        x.replace('"', '') if isinstance(x, str)
                                                        else x)
        return source_value[references]
    elif isinstance(source_value, list):
        return pd.DataFrame(source_value, columns=references)
    elif isinstance(source_value, tuple):
        return pd.DataFrame(list(source_value), columns=references)
    elif isinstance(source_value, dict):
        return _read_inmemory_json(json.dumps(source_value), rml_rule, references)
    elif _check_if_json(source_value):
        return _read_inmemory_json(source_value, rml_rule, references)
    else:
        raise ValueError(f'Found an invalid in-memory data structure.')


def _check_if_json(json):
    try:
        json.loads(json)
    except ValueError as e:
        return False
    return True


def _read_inmemory_json(source_value, rml_rule, references):
    json_data = json.loads(source_value)

    jsonpath_expression = rml_rule['iterator'] + '.('
    # add top level object of the references to reduce intermediate results (THIS IS NOT STRICTLY NECESSARY)
    for reference in references:
        jsonpath_expression += reference + ','
    jsonpath_expression = jsonpath_expression[:-1] + ')'

    jsonpath_result = JSONPath(jsonpath_expression).parse(json_data)
    # normalize and remove nulls
    json_df = pd.json_normalize([json_object for json_object in normalize_hierarchical_data(jsonpath_result) if
                                 None not in json_object.values()])

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(json_df.columns)))
    json_df[missing_references_in_df] = np.nan

    return json_df
