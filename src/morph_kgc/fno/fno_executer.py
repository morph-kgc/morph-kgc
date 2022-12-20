__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import sys
import logging

from importlib.machinery import SourceFileLoader

from .built_in_vectorized import biv_dict
from .built_in_scalar import bis_dict
from ..utils import get_fno_execution
from ..constants import FNML_EXECUTION, R2RML_TEMPLATE


def load_udfs(config):
    if config.get_udfs():
        udf_module = SourceFileLoader("udf", config.get_udfs()).load_module()
        udf_dict = udf_module.udf_dict
    else:
        udf_dict = {}

    return udf_dict


def execute_fno(data, fno_df, fno_execution, config):
    execution_rule_df = get_fno_execution(fno_df, fno_execution)
    function_id = execution_rule_df.iloc[0]['function_map_value']
    execution_id = execution_rule_df.iloc[0]['execution']

    # handle composite functions
    for i, execution_rule in execution_rule_df.iterrows():
        if execution_rule['value_map_type'] == FNML_EXECUTION:
            data = execute_fno(data, fno_df, execution_rule['value_map_value'], config)
        elif execution_rule['value_map_type'] == R2RML_TEMPLATE:
            logging.error('Value maps that are rr:template are not supported yet.')
            sys.exit()

    parameter_to_value_dict = dict(zip(execution_rule_df['parameter_map_value'], execution_rule_df['value_map_value']))

    # prepare function and execute
    if function_id in biv_dict:
        function = biv_dict[function_id]['function']
        function_parameters = biv_dict[function_id]['parameters']

        for key, value in function_parameters.items():
            function_parameters[key] = data[parameter_to_value_dict[value]]
        data[execution_id] = function(**function_parameters)
    else:
        if function_id in bis_dict:
            function = bis_dict[function_id]['function']
            function_parameters = bis_dict[function_id]['parameters']
        else:
            udf_dict = load_udfs(config)
            function = udf_dict[function_id]['function']
            function_parameters = udf_dict[function_id]['parameters']

        for key, value in function_parameters.items():
            function_parameters[key] = parameter_to_value_dict[value]

        for i, row in data.iterrows():
            params = {}
            for key, value in function_parameters.items():
                params[key] = row[value]
            data.at[i, execution_id] = function(**params)

    return data
