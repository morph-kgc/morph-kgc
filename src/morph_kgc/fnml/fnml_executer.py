__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd

from .built_in_functions import bif_dict
from ..utils import get_fnml_execution, remove_null_values_from_dataframe, get_references_in_template
from ..constants import RML_EXECUTION, RML_TEMPLATE, RML_CONSTANT


UDF_DICT_DECORATOR_CODE = """
udf_dict = {}
def udf(fun_id, **params):
    def wrapper(funct):
        udf_dict[fun_id] = {}
        udf_dict[fun_id]['function'] = funct
        udf_dict[fun_id]['parameters'] = params
        return funct
    return wrapper
"""

def load_udfs(config):
    """
    Loads user-defined functions (UDFs) from a file specified in the given configuration.
    This function reads the UDFs file path from the `config` object, appends a decorator 
    code to the UDFs, dynamically creates a Python module to execute the UDFs code, and 
    returns a dictionary of the defined UDFs.
    Args:
        config: An object that provides access to configuration settings. It must have 
                a `get_udfs()` method that returns the file path to the UDFs or None 
                if no UDFs are specified.
    Returns:
        dict: A dictionary containing the UDFs defined in the file. If no UDFs file is 
              specified in the configuration, an empty dictionary is returned.
    Raises:
        FileNotFoundError: If the UDFs file specified in the configuration does not exist.
        SyntaxError: If there is a syntax error in the UDFs code.
    """
    if config.get_udfs():
        import sys
        from types import ModuleType

        with open(config.get_udfs(), 'r') as f:
            udfs_code = f.read()

        udfs_code = f'{UDF_DICT_DECORATOR_CODE}{udfs_code}'

        udf_mod = ModuleType('udfs')
        sys.modules['udfs'] = udf_mod
        exec(udfs_code, udf_mod.__dict__)

        return udf_mod.udf_dict
    else:
        return {}


def _materialize_fnml_template(data, template):
    # TODO: this function is very similar to _materialize_template in materializer
    references = get_references_in_template(template)

    # Curly braces that do not enclose column names MUST be escaped by a backslash character (“\”).
    # This also applies to curly braces within column names.
    template = template.replace('\\{', '{').replace('\\}', '}')

    # use auxiliary column to store the data of the template
    data['aux_fnml_template_data'] = ''

    for reference in references:
        data['reference_results'] = data[reference]

        splitted_template = template.split('{' + reference + '}')
        data['aux_fnml_template_data'] = data['aux_fnml_template_data'] + splitted_template[0] + data[
            'reference_results']
        template = str('{' + reference + '}').join(splitted_template[1:])
    if template:
        # add what remains in the template after the last reference
        data['aux_fnml_template_data'] = data['aux_fnml_template_data'] + template

    return data['aux_fnml_template_data']


def execute_fnml(data:pd.DataFrame, fnml_df: pd.DataFrame, fnml_execution:dict, config, in_recursion=False):
    """
    Executes an FNML (Function-based Mapping Language) transformation on the provided data.
    Args:
        data (pd.DataFrame): The input data to be transformed.
        fnml_df (pd.DataFrame): The FNML mapping definitions as a DataFrame.
        fnml_execution (dict): The execution context (an id) for the FNML transformation.
        config: Configuration object containing settings and parameters for the execution.
        in_recursion (bool, optional): Indicates whether the function is being called recursively. Defaults to False.
    Returns:
        pd.DataFrame: The transformed data after applying the FNML mappings and functions.
    Notes:
        - Handles composite functions by recursively calling itself for nested executions.
        - Supports functions with multiple parameters that need to be aggregated into arrays.
        - Dynamically loads user-defined functions (UDFs) if the function ID is not a built-in function.
        - Prepares function parameters based on their mapping type (e.g., constant, template, reference, or execution).
        - Executes the specified function for each row of the input data.
        - Removes null values from the resulting DataFrame and optionally explodes list values for outer functions.
    Raises:
        KeyError: If a required function or parameter is not found in the mappings or configuration.
        Exception: If an error occurs during function execution or data transformation.
    """
    execution_rule_df = get_fnml_execution(fnml_df, fnml_execution)
    function_id = execution_rule_df.iloc[0]['function_map_value']
    # handle composite functions
    for i, execution_rule in execution_rule_df.iterrows():
        if execution_rule['value_map_type'] == RML_EXECUTION:
            data = execute_fnml(data, fnml_df, execution_rule['value_map_value'], config, True)

    parameter_to_value_type_dict = dict(
        zip(execution_rule_df['parameter_map_value'], execution_rule_df['value_map_type']))
    parameter_to_value_value_dict = dict(
        zip(execution_rule_df['parameter_map_value'], execution_rule_df['value_map_value']))
    # get keys with mutliple paramters that should be converted into an array.
    functions_with_arrayparameter = fnml_df.groupby(["function_execution", "function_map_value", "parameter_map_value"]).count()["value_map_value"].reset_index()
    # functions_with_arrayparameter = functions_with_arrayparameter[functions_with_arrayparameter.value_map_value > 1]
    if len(functions_with_arrayparameter) > 0:
        for param in functions_with_arrayparameter[functions_with_arrayparameter.function_execution == fnml_execution].parameter_map_value.to_list():
            parameter_to_value_value_dict[param] = fnml_df[(fnml_df.function_execution == fnml_execution) & (fnml_df.parameter_map_value == param)].value_map_value.to_list()
            parameter_to_value_type_dict[param] = fnml_df[(fnml_df.function_execution == fnml_execution) & (fnml_df.parameter_map_value == param)].value_map_type.to_list()
    # prepare function and execute
    if function_id in bif_dict:
        function = bif_dict[function_id]['function']
        function_decorator_parameters = bif_dict[function_id]['parameters']
    else:
        user_defined_functions = load_udfs(config)
        function = user_defined_functions[function_id]['function']
        function_decorator_parameters = user_defined_functions[function_id]['parameters']
    function_param_array = []
    for function_parameter_name, function_parameter_value in function_decorator_parameters.items():       
        if function_parameter_value in parameter_to_value_type_dict:
            for i in range(len(parameter_to_value_value_dict[function_parameter_value])):
                tmp_functionparameter_dict = {}
                if parameter_to_value_type_dict[function_parameter_value][i] == RML_CONSTANT:
                    tmp_functionparameter_dict.update({function_parameter_name : [parameter_to_value_value_dict[function_parameter_value][i]] * len(data)})
                elif parameter_to_value_type_dict[function_parameter_value][i] == RML_TEMPLATE:
                    fnml_template_data = _materialize_fnml_template(data, parameter_to_value_value_dict[function_parameter_value][i])
                    tmp_functionparameter_dict.update({function_parameter_name : list(fnml_template_data)})
                else:
                    # RML_REFERENCE or RML_EXECUTION
                    tmp_functionparameter_dict.update({function_parameter_name: list(data[parameter_to_value_value_dict[function_parameter_value][i]])})
                if i in range(len(function_param_array)):
                    function_param_array[i].update(tmp_functionparameter_dict)
                else:
                    function_param_array.append(tmp_functionparameter_dict)

                
    # Previously, we worked with arrays to enable an input of various heterogenous array values in the input.
    # Now, we restructure it to the previous array structure to enable the use of the function in the same way as before.
    function_params = {}
    if len(function_param_array)> 0:
        for key in function_param_array[0]:
            restructured_innerarray = []
            for j in range(len(function_param_array[0][key])):
                restructured_inner2array = []
                for i in range(len(function_param_array)):
                    restructured_inner2array.append(function_param_array[i][key][j])
                restructured_innerarray.append(restructured_inner2array if len(restructured_inner2array) > 1 else restructured_inner2array[0])
            function_params[key] = restructured_innerarray
    exec_res = []
    for i in range(len(data)):
        exec_params = {}
        for function_parameter_name, function_parameter_value in function_params.items():
            exec_params[function_parameter_name] = function_parameter_value[i]
        exec_res.append(function(**exec_params))

    data[fnml_execution] = exec_res

    # TODO: this can be avoided for many built-in functions and also UDFs with a special parameter
    #if function_id in ['http://users.ugent.be/~bjdmeest/function/grel.ttl#string_split']:

    data = remove_null_values_from_dataframe(data, config, fnml_execution, column=fnml_execution)

    # Only explode the results of an outer function. Otherwise, if the functions are nested, the inner function should work with the list object.
    if not in_recursion:
        # only list values are exploded, strings that encode lists are not exploded
        data = data.explode(fnml_execution)

    return data
