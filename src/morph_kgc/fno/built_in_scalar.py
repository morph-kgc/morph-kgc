__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


from uuid import uuid4


bis_dict = {}


def bis(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        bis_dict[fun_id] = {}
        bis_dict[fun_id]['function'] = funct
        bis_dict[fun_id]['parameters'] = params
        return funct
    return wrapper


##############################################################################
########################   GREL   ############################################
##############################################################################




##############################################################################
########################   OTHER   ###########################################
##############################################################################


@bis(
    fun_id='https://github.com/oeg-upm/morph-kgc/function/built-in.ttl#uuid')
def uuid():
    return str(uuid4())
