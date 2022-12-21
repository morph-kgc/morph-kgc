__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


biv_dict = {}


##############################################################################
########################   BUILT-IN VECTORIZED FUNCTION DECORATOR   ##########
##############################################################################


def biv(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        biv_dict[fun_id] = {}
        biv_dict[fun_id]['function'] = funct
        biv_dict[fun_id]['parameters'] = params
        return funct
    return wrapper


##############################################################################
########################   GREL   ############################################
##############################################################################


@biv(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toLowerCase',
    text_series='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_lower_case(text_series):
    return text_series.str.lower()


@biv(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toUpperCase',
    text_series='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_upper_case(text_series):
    return text_series.str.upper()


##############################################################################
########################   OTHER   ###########################################
##############################################################################
