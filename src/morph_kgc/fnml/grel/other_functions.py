from ..function_decorator import *
from uuid import uuid4
from falcon.uri import encode_value
import hashlib


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
    return hashlib.md5(string.encode()).hexdigest()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_sha1",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_sha1(string):
    return hashlib.sha1(string.encode()).hexdigest()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#unicodestring-s",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_unicode(string):
    return [ord(e) for e in string]

@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#other_type",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_any_e",
)
def other_type(string):
    return type(string).__name__


@bif(
    fun_id="http://example.com/idlab/function/toUpperCaseURL",
    url="http://example.com/idlab/function/str",
)
def to_upper_case_url(url):
    url_lower = url.lower()

    if url_lower.startswith("https://"):
        return f"https://{encode_value(url[:8].upper())}"
    elif url_lower.startswith("http://"):
        return f"http://{encode_value(url[:7].upper())}"

    # else:
    return f"http://{encode_value(url.upper())}"


@bif(fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#uuid")
def uuid():
    return str(uuid4())

@bif(fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#other_coalesce",
     p_any_rep_element="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_any_rep_e",)
def coalesce(p_any_rep_element):
    for e in p_any_rep_element:
        if e is not None and e != "":
            return e


# Todo: Describe in Function Description
@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#string_split_explode",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    separator="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_sep",
)
def string_split_explode(string, separator):
    return string.split(separator)
