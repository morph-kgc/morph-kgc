from ..function_decorator import *
import re

@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#escape",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    mode="http://users.ugent.be/~bjdmeest/function/grel.ttl#modeParameter",
)
def string_escape(string, mode):
    if mode == "html":
        import html

        return html.escape(string)
    elif mode == "xml":
        from xml.sax.saxutils import escape

        return escape(string)
    elif mode == "url":
        import urllib.parse

        return urllib.parse.quote(string, safe="")
    elif mode == "javascript":
        import json

        return json.dumps(string)[1:-1]
    elif mode == "csv":
        import csv
        import io

        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        writer.writerow([string])
        return output.getvalue().strip()
    else:
        # TODO: not valid mode
        pass

@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_contains",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_sub",
)
def string_contains(string, substring):
    return str(substring in string).lower()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_unescape",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    mode="http://users.ugent.be/~bjdmeest/function/grel.ttl#modeParameter",
)
def string_unescape(string, mode):
    if mode == "html":
        import html

        return html.unescape(string)
    elif mode == "xml":
        from xml.sax.saxutils import unescape

        return unescape(string)
    elif mode == "url":
        import urllib.parse

        return urllib.parse.unquote(string)
    elif mode == "javascript":
        import json

        return json.loads(string)[1:-1]
    else:
        # TODO: not valid mode
        pass


# Not defined by GREL
@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_indexOf",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_sub",
)
def string_index_of(string, substring):
    return string.index(substring) if substring in string else -1


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_lastIndexOf",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_sub",
)
def string_lastindex_of(string: str, substring: str):
    return string.rindex(substring) if substring in string else -1


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_toNumber",
    any="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_any_e",
    
)
def string_lastindex_of(any: str):
    return float(any) if any else None


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_toString",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_any_e",
)
def string_to_string(string):
    return str(string)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_diff",
    diff_string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter2",
)
def string_diff(string: str, diff_string: str):
    return string if not string.startswith(diff_string) else string[len(diff_string) :]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_length",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_length(string):
    return str(len(string))


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_splitByLengths",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    i_1="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i  ",
    i_2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i2 ",
    rep_i="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_rep_i  ",
)
def string_split_by_lengths(string, i_1, i_2, p_rep_i):
    split_1 = string[: int(i_1)]
    split_2 = string[int(i_1) : int(i_1) + int(i_2)]
    split_3 = string[int(i_1) + int(i_2) : int(i_1) + int(i_2) + (int(p_rep_i))]
    return str([split_1, split_2, split_3])


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_split",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    separator="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_sep",
)
@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_smartSplit",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    separator="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_opt_sep",
)
def string_split(string, separator=None):
    if separator is None:
        if "\t" in string:
            separator = "\t"
        elif "," in string:
            separator = ","
        else:
            return string
    return string.split(separator)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_substring",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    param_int_i_from="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_int_i_from",
    param_int_i_opt_to="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_int_i_opt_to",
)
def string_sub_string(string, param_int_i_from, param_int_i_opt_to=None):
    print(
        f"param_int_i_from: {param_int_i_from}, param_int_i_opt_to: {param_int_i_opt_to}"
    )
    param_int_i_from = int(param_int_i_from)
    param_int_i_opt_to = int(param_int_i_opt_to) if param_int_i_opt_to else None
    return string[param_int_i_from:param_int_i_opt_to]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_splitByCharType",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_split_by_chartype(string):
    return re.findall(r"[A-Z]+|[a-z]+|\s|[0-9]", string)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_partition",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    fragment="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_fragment",
    omit_fragment="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_bool_opt_b",
)
def string_partition(string:str, fragment:str, omit_fragment=False):
    if fragment not in string:
        return ["", string]
    if str(omit_fragment).lower() == "false":
        return list(string.partition(fragment))
    else:
        parts = string.partition(fragment)
        return [parts[0], parts[2]]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_rpartition",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    fragment="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_fragment",
    omit_fragment="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_bool_opt_b",
)
def string_rpartition(string: str, fragment, omit_fragment=False):
    if fragment not in string:
        return ["", string]
    if str(omit_fragment).lower() == "false":
        return list(string.rpartition(fragment))
    else:
        parts = string.rpartition(fragment)
        return [parts[0], parts[2]]
    
    
@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_contains",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_sub",
)
def string_contains(string, substring):
    return str(substring in string).lower()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_chomp",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    separator="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_sep",
)
def string_contains(string: str, separator: str):
    return (
        string[0 : len(string) - len(separator)]
        if str(string).endswith(separator)
        else string
    )

@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#reverse",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def reverse(string):
    return string[::-1]




@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_replace",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    old_substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_find",
    new_substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_replace",
)
@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_replaceChars",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    old_substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_find",
    new_substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_replace",
)
def string_replace(string, old_substring, new_substring):
    return string.replace(old_substring, new_substring)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_match",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    regex="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_regex",
)
def string_match(string, regex):
    return (
        re.findall(regex[1:-1], string)
        if regex[0] == "/" and regex[-1] == "/"
        else re.findall(regex, string)
    )


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
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_trim",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_trim(string: str):
    return string.strip()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_startsWith",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_sub",
)
def string_starts_with(string: str, substring: str):
    return str(string.startswith(substring)).lower()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_endsWith",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    substring="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_sub",
)
def string_ends_with(string: str, substring: str):
    return str(string.endswith(substring)).lower()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_trim",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_trim(string: str):
    return string.strip()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#toLowerCase",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def to_lower_case(string):
    return string.lower()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#toUpperCase",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def to_upper_case(string):
    return string.upper()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#string_toTitlecase",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def to_title_case(string: str):
    return string.title()


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
