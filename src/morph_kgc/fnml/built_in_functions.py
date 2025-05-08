__author__ = "Julián Arenas-Guerrero"
import re

__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

from .grel.string_functions import bif_dict as string_bif
from .grel.array_functions import bif_dict as array_bif
from .grel.date_functions import bif_dict as date_bif
from .grel.control_functions import bif_dict as control_bif
from .function_decorator import bif

bif_dict = {}
bif_dict.update(string_bif)
bif_dict.update(array_bif)
bif_dict.update(date_bif)
bif_dict.update(control_bif)


##############################################################################
########################   BUILT-IN SCALAR FUNCTION DECORATOR   ##############
##############################################################################







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
