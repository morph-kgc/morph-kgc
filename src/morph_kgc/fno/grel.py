__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


grel_dict = {}


def grel(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        grel_dict[fun_id] = {}
        grel_dict[fun_id]['function'] = funct
        grel_dict[fun_id]['parameters'] = params
        return funct
    return wrapper


@grel(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toLowercase',
    text_series='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter')
def to_lower_case(text_series):
    return text_series.str.lower()


@grel(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toUppercase',
    text_series='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter')
def to_upper_case(text_series):
    return text_series.str.upper()


@grel(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#concat',
    text_series_1='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter1',
    text_series_2='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter2')
def concat(text_series_1, text_series_2):
    return text_series_1 + text_series_2