__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


from .built_in_functions import bif_dict
from ..utils import get_fno_execution, remove_null_values_from_dataframe, get_references_in_template
from ..constants import FNML_EXECUTION, R2RML_TEMPLATE, R2RML_CONSTANT


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


def _materialize_fno_template(data, template):
    # TODO: this function is very similar to _materialize_template in materializer
    references = get_references_in_template(template)

    # Curly braces that do not enclose column names MUST be escaped by a backslash character (“\”).
    # This also applies to curly braces within column names.
    template = template.replace('\\{', '{').replace('\\}', '}')

    # use auxiliary column to store the data of the template
    data['aux_fno_template_data'] = ''

    for reference in references:
        data['reference_results'] = data[reference]

        splitted_template = template.split('{' + reference + '}')
        data['aux_fno_template_data'] = data['aux_fno_template_data'] + splitted_template[0] + data['reference_results']
        template = str('{' + reference + '}').join(splitted_template[1:])
    if template:
        # add what remains in the template after the last reference
        data['aux_fno_template_data'] = data['aux_fno_template_data'] + template

    return data['aux_fno_template_data']


def execute_fno(data, fno_df, fno_execution, config):
    execution_rule_df = get_fno_execution(fno_df, fno_execution)
    function_id = execution_rule_df.iloc[0]['function_map_value']

    # handle composite functions
    for i, execution_rule in execution_rule_df.iterrows():
        if execution_rule['value_map_type'] == FNML_EXECUTION:
            data = execute_fno(data, fno_df, execution_rule['value_map_value'], config)

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
    for k, v in function_decorator_parameters.items():

        # if parameter is optional it is not in parameter_to_value_type_dict
        if v in parameter_to_value_type_dict:
            if parameter_to_value_type_dict[v] == R2RML_CONSTANT:
                function_params[k] = [parameter_to_value_value_dict[v]] * len(data)
            elif parameter_to_value_type_dict[v] == R2RML_TEMPLATE:
                fno_template_data = _materialize_fno_template(data, parameter_to_value_value_dict[v])
                function_params[k] = list(fno_template_data)
            else:
                # RML_REFERENCE or FNML_EXECUTION
                function_params[k] = list(data[parameter_to_value_value_dict[v]])

    exec_res = []
    for i in range(len(data)):
        exec_params = {}
        for k, v in function_params.items():
            exec_params[k] = v[i]
        exec_res.append(function(**exec_params))

    data[fno_execution] = exec_res

    # TODO: this can be avoided for many built-in functions and also UDFs with a special parameter
    #if function_id in ['http://users.ugent.be/~bjdmeest/function/grel.ttl#string_split']:

    data = remove_null_values_from_dataframe(data, config, fno_execution, column=fno_execution)

    # only list values are exploded, strings that encode lists are not exploded
    data = data.explode(fno_execution)

    return data
