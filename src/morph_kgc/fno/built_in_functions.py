__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


bif_dict = {}


##############################################################################
########################   BUILT-IN SCALAR FUNCTION DECORATOR   ##############
##############################################################################


def bif(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        bif_dict[fun_id] = {}
        bif_dict[fun_id]['function'] = funct
        bif_dict[fun_id]['parameters'] = params
        return funct
    return wrapper


##############################################################################
########################   GREL   ############################################
##############################################################################


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#escape',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    mode='http://users.ugent.be/~bjdmeest/function/grel.ttl#modeParam')
def string_escape(string, mode):
    if mode == 'html':
        import html
        return html.escape(string)
    else:
        # TODO: not valid mode
        pass


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#string_toString',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_any_e')
def string_to_string(string):
    return str(string)


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#date_toDate',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    format_code='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_string_pattern')
def date_to_date(string, format_code):
    from datetime import datetime

    return str(datetime.strptime(string, format_code).date())


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#string_split',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    separator='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_string_sep')
def string_split(string, separator):
    return str(string.split(separator))


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#array_get',
    string_list='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_a',
    start='http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i_from',
    end='http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i_opt_to')
def string_array_get(string_list, start, end=None):
    # it does not explode

    try:
        string_list = eval(string_list) # it is a list
    except:
        pass # it is a string

    start = int(start)
    if end:
        end = int(end)
        return str(string_list[start:end])
    else:
        return string_list[start]


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#array_slice',
    string_list='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_a',
    start='http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i_from',
    end='http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i_opt_to')
def string_array_slice(string_list, start, end=None):
    # it does not explode

    try:
        string_list = eval(string_list) # it is a list
    except:
        pass # it is a string

    start = int(start)
    if end:
        end = int(end)
        return str(string_list[start:end])
    else:
        return str(string_list[start:])


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#string_replace',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    old_substring='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_find',
    new_substring='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_replace')
def string_replace(string, old_substring, new_substring):
    return string.replace(old_substring, new_substring)


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toLowerCase',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_lower_case(string):
    return string.lower()


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toUpperCase',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_upper_case(string):
    return string.upper()


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#toTitleCase',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_title_case(string):
    return string.title()


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#string_trim',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def string_trim(string):
    return string.strip()


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#controls_if',
    boolean_expression='http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b',
    value_true='http://users.ugent.be/~bjdmeest/function/grel.ttl#any_true',
    value_false='http://users.ugent.be/~bjdmeest/function/grel.ttl#any_false')
def controls_if(boolean_expression, value_true, value_false=None):
    if eval(boolean_expression):
        return value_true
    else:
        return value_false


@bif(
    fun_id='http://users.ugent.be/~bjdmeest/function/grel.ttl#math_round',
    number='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_dec_n')
def number_round(number):
    if ','  in number and '.' in number:
        number = number.replace(',', '')    # e.g. 4,894.57
    elif ',' in number:
        number = number.replace(',', '.')   # e.g. 10,7

    return str(round(float(number)))


##############################################################################
########################   OTHER   ###########################################
##############################################################################


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#controls_if_cast',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b',
    value_true='http://users.ugent.be/~bjdmeest/function/grel.ttl#any_true',
    value_false='http://users.ugent.be/~bjdmeest/function/grel.ttl#any_false')
def controls_if_cast(string, value_true, value_false=None):
    if string.lower() in ['', 'false', 'no', 'off', '0']:
        # this will be filtered when removing nulls
        return value_false
    else:
        return value_true


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#uuid')
def uuid():
    from uuid import uuid4

    return str(uuid4())


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#string_split_explode',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    separator='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_string_sep')
def string_split_explode(string, separator):
    return string.split(separator)


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#concat',
    string1='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam1',
    string2='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam2',
    separator='http://users.ugent.be/~bjdmeest/function/grel.ttl#param_string_sep')
def string_concat(string1, string2, separator=''):
    return f'{string1}{separator}{string2}'


@bif(
    fun_id='http://example.com/idlab/function/toUpperCaseURL',
    url='http://example.com/idlab/function/str')
def to_upper_case_url(url):
    from falcon.uri import encode_value

    url_lower = url.lower()

    if url_lower.startswith('https://'):
        return f'https://{encode_value(url[:8].upper())}'
    elif url_lower.startswith('http://'):
        return f'http://{encode_value(url[:7].upper())}'

    # else:
    return f'http://{encode_value(url.upper())}'


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#hash',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def hash(string):
    from hashlib import sha256
    return sha256(string.encode("UTF-8")).hexdigest()


@bif(
    fun_id='https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#hash_iri',
    string='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def hash_iri(string):
    return f'http://example.com/ns#{sha256(string.encode("UTF-8")).hexdigest()}'
