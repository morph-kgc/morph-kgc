__author__ = "Julián Arenas-Guerrero"
import re

__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

from .grel.string_functions import bif_dict as string_bif
from .grel.array_functions import bif_dict as array_bif
from .function_decorator import bif

bif_dict = {}
bif_dict.update(string_bif)
bif_dict.update(array_bif)

##############################################################################
########################   BUILT-IN SCALAR FUNCTION DECORATOR   ##############
##############################################################################



@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_toDate",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    format_code="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_pattern",
)
def date_to_date(string, format_code):
    from datetime import datetime

    return str(datetime.strptime(string, format_code).date())




@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#reverse",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def reverse(string):
    return string[::-1]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#controls_if",
    boolean_expression="http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b",
    value_true="http://users.ugent.be/~bjdmeest/function/grel.ttl#any_true",
    value_false="http://users.ugent.be/~bjdmeest/function/grel.ttl#any_false",
)
def controls_if(boolean_expression, value_true, value_false=None):
    if eval(boolean_expression):
        return value_true
    else:
        return value_false


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_round",
    number="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def number_round(number):
    if "," in number and "." in number:
        number = number.replace(",", "")  # e.g. 4,894.57
    elif "," in number:
        number = number.replace(",", ".")  # e.g. 10,7

    return str(round(float(number)))


##############################################################################
########################   OTHER   ###########################################
##############################################################################


@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#controls_if_cast",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b",
    value_true="http://users.ugent.be/~bjdmeest/function/grel.ttl#any_true",
    value_false="http://users.ugent.be/~bjdmeest/function/grel.ttl#any_false",
)
def controls_if_cast(string, value_true, value_false=None):
    if string.lower() in ["", "false", "no", "off", "0"]:
        # this will be filtered when removing nulls
        return value_false
    else:
        return value_true


@bif(fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#uuid")
def uuid():
    from uuid import uuid4

    return str(uuid4())


# Todo: Describe in Function Description
@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#string_split_explode",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    separator="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_sep",
)
def string_split_explode(string, separator):
    return string.split(separator)


# Todo: Describe in Function Description
@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#concat",
    string1="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter1",
    string2="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter2",
    separator="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_sep",
)
def string_concat(string1, string2, separator=""):
    return f"{string1}{separator}{string2}"


@bif(
    fun_id="http://example.com/idlab/function/toUpperCaseURL",
    url="http://example.com/idlab/function/str",
)
def to_upper_case_url(url):
    from falcon.uri import encode_value

    url_lower = url.lower()

    if url_lower.startswith("https://"):
        return f"https://{encode_value(url[:8].upper())}"
    elif url_lower.startswith("http://"):
        return f"http://{encode_value(url[:7].upper())}"

    # else:
    return f"http://{encode_value(url.upper())}"


# Hashs
@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#hash",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def hash(string):
    from hashlib import sha256

    return sha256(string.encode("UTF-8")).hexdigest()


@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#hash_iri",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def hash_iri(string):
    return f'http://example.com/ns#{sha256(string.encode("UTF-8")).hexdigest()}'


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_md5",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_md5(string):
    import hashlib

    return hashlib.md5(string.encode()).hexdigest()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_sha1",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_sha1(string):
    import hashlib

    return hashlib.sha1(string.encode()).hexdigest()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#unicodestring-s",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_unicode(string):
    return [ord(e) for e in string]
