__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import sys
import logging

import pandas as pd

from importlib.machinery import SourceFileLoader

from .built_in_functions import bif_dict
from ..utils import get_fno_execution
from ..constants import FNML_EXECUTION, R2RML_TEMPLATE, R2RML_CONSTANT


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

    parameter_to_value_type_dict = dict(zip(execution_rule_df['parameter_map_value'], execution_rule_df['value_map_type']))
    parameter_to_value_value_dict = dict(zip(execution_rule_df['parameter_map_value'], execution_rule_df['value_map_value']))

    # prepare function and execute
    if function_id in bif_dict:
        function = bif_dict[function_id]['function']
        function_decorator_parameters = bif_dict[function_id]['parameters']
    else:
        udf_dict = load_udfs(config)
        function = udf_dict[function_id]['function']
        function_decorator_parameters = udf_dict[function_id]['parameters']

    function_params = {}
    for key, value in function_decorator_parameters.items():
        if parameter_to_value_type_dict[value] == R2RML_CONSTANT:
            function_params[key] = [parameter_to_value_value_dict[value]] * len(data)
        else:
            # TODO: what if template?
            function_params[key] = list(data[parameter_to_value_value_dict[value]])

    exec_res = []
    for i in range(len(data)):
        exec_params = {}
        for k, v in function_params.items():
            exec_params[k] = v[i]
        exec_res.append(function(**exec_params))
    data[execution_id] = pd.Series(exec_res)

    # TODO: remove null values if the execution of the function introduced them

    return data

