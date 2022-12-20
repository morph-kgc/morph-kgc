__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


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
    text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter')
def to_lower_case(text):
    return text.lower()


@fno(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toUppercase',
    text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter')
def to_upper_case(text):
    return text.upper()


@fno(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#strip',
    text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter')
def strip(text):
    return text.strip()