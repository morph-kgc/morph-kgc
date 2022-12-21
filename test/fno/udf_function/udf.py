__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


udf_dict = {}


def udf(fun_id, **params):
    def wrapper(funct):
        udf_dict[fun_id] = {}
        udf_dict[fun_id]['function'] = funct
        udf_dict[fun_id]['parameters'] = params
        return funct
    return wrapper


@udf(
    fun_id='http://example.com/toUpperCase',
    text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_upper_case(text):
    return text.upper()
