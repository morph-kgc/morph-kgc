__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import html

from uuid import uuid4
from falcon.uri import encode_value


bis_dict = {}


##############################################################################
########################   BUILT-IN SCALAR FUNCTION DECORATOR   ##############
##############################################################################


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


@bis(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#escape',
    text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter',
    mode='http://users.ugent.be/~bjdmeest/function/grel.ttl#modeParameter')
def escape(text, mode):
    if mode == 'html':
        return html.escape(text)
    else:
        raise


##############################################################################
########################   OTHER   ###########################################
##############################################################################


@bis(
    fun_id='https://github.com/oeg-upm/morph-kgc/function/built-in.ttl#uuid')
def uuid():
    return str(uuid4())


@bis(
    fun_id='http://example.com/idlab/function/toUpperCaseURL',
    url='http://example.com/idlab/function/str')
def to_upper_case_url(url):
    url_lower = url.lower()

    if url_lower.startswith('https://'):
        return f'https://{encode_value(url[:8].upper())}'
    elif url_lower.startswith('http://'):
        return f'http://{encode_value(url[:7].upper())}'

    # else:
    return f'http://{encode_value(url.upper())}'

