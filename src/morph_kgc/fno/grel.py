__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd


functions_dict = {}


def fno(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        functions_dict[fun_id] = {}
        functions_dict[fun_id]['function'] = funct
        functions_dict[fun_id]['parameters'] = params
        return funct
    return wrapper


@fno(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toLowercase',
    text_series='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter')
def to_lower_case(text_series):
    return text_series.str.lower()


@fno(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toUppercase',
    text_series='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter')
def to_upper_case(text_series):
    return text_series.str.upper()


@fno(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#strip',
    text_series='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter')
def strip(text_series):
    return text_series.str.strip()


@fno(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#concat',
    text_series_1='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter1',
    text_series_2='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter2')
def concat(text_series_1, text_series_2):
    return text_series_1 + text_series_2