__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import math
import random
import re
import hashlib

from functools import reduce
from operator import xor
from datetime import datetime
from datetime import timedelta
from ast import literal_eval
from uuid import uuid4
from falcon.uri import encode_value


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
########################   ARRAY   ###########################################
##############################################################################


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_get",
    string_list="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
    start="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_int_i_from",
    end="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_int_i_opt_to",
)
def array_get(string_list, start, end=None):
    # it does not explode

    try:
        string_list = eval(string_list)  # it is a list
    except:
        pass  # it is a string

    start = int(start)
    if end:
        end = int(end)
        return string_list[start:end]
    else:
        return string_list[start]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_length",
    array_list="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
)
def array_length(array_list):
    if type(array_list) != list:
        return None
    else:
        return len(array_list)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_sum",
    array_list="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
)
def array_sum(array_list):
    print(array_list)
    print(type(array_list))
    if type(array_list) != list:
        return None
    else:
        sum = 0
        for e in array_list:

            try:
                sum += literal_eval(e)
            except:
                pass
        # allows us to cast back a float to an int.
        return literal_eval(str(sum))


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_slice",
    string_list="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
    start="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_int_i_from",
    end="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_int_i_opt_to",
)
def array_slice(string_list, start, end=None):
    # it does not explode

    try:
        string_list = eval(string_list)  # it is a list
    except:
        pass  # it is a string

    start = int(start)
    if end:
        end = int(end)
        return str(string_list[start:end])
    else:
        return str(string_list[start:])


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_join",
    array="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
    p_string_sep="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_sep",

)
def array_join(array: list | str, p_string_sep):
    # it does not explode
    if type(array) != list:
        return array
    response = ""
    for e in array:
        response += str(e) + p_string_sep
    return response[:-len(p_string_sep)]  # remove last separator


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_reverse",
    array="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
)
def array_reverse(array: list):
    # it does not explode
    if type(array) != list:
        return array
    return array[::-1]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_uniques",
    array="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
)
def array_uniques(array: list):
    return [x for i, x in enumerate(array) if x not in array[:i]]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_sort",
    array="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
)
def array_sort(array: list):
    if type(array) != list:
        return None
    array.sort()
    return array


##############################################################################
########################   CONTROL   #########################################
##############################################################################


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#controls_if",
    boolean_expression="http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b",
    value_true="http://users.ugent.be/~bjdmeest/function/grel.ttl#any_true",
    value_false="http://users.ugent.be/~bjdmeest/function/grel.ttl#any_false",
)
def controls_if(boolean_expression, value_true, value_false=None):
    if str(boolean_expression).lower() in ["true", 1]:
        return value_true
    elif str(boolean_expression).lower() in ["false", 0]:
        return value_false
    else:
        if eval(boolean_expression):
            return value_true
        else:
            return value_false


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
    elif string.lower() in ["true", "yes", "on", "1"]:
        return value_true
    else:
        if eval(string):
            return value_true
        else:
            return value_false


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#boolean_and",
    bool_input="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_rep_b",

)
def boolean_and(bool_input: list | str):
    if type(bool_input) == str:
        bool_input = [bool_input]
    return str(all([True if i.lower() in ["true", 1] else False for i in bool_input])).lower()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#boolean_or",
    bool_input="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_rep_b",

)
def boolean_and(bool_input: list | str):
    if type(bool_input) == str:
        bool_input = [bool_input]
    return str(any([True if i.lower() in ["true", 1] else False for i in bool_input])).lower()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#boolean_xor",
    bool_input="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_rep_b",

)
def boolean_xor(bool_input: list | str):
    if type(bool_input) == str:
        bool_input = [bool_input]
    return str(reduce(xor, [True if i.lower() in ["true", 1] else False for i in bool_input])).lower()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#boolean_not",
    bool_input="http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b",

)
def boolean_not(bool_input):
    return str(not (True if bool_input.lower() in ["true", 1] else False)).lower()


##############################################################################
########################   DATE   ############################################
##############################################################################


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#unicodestring-s",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_unicode(string):
    return [ord(e) for e in string]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_toDate",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    format_code="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_pattern",
)
def date_to_date(string, format_code):
    from datetime import datetime

    # Map GREL format to Python format
    grel_to_python_format = {
        "yyyy": "%Y",
        "yy": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "dd": "%d",
        "EEEE": "%A",
        "EEE": "%a",
        "HH": "%H",
        "hh": "%I",
        "mm": "%M",
        "ss": "%S",
        "a": "%p",
        "Z": "%z",
        "X": "%Z",
        "SSS": "%f",
    }
    for grel, py in grel_to_python_format.items():
        format_code = format_code.replace(grel, py)
    return datetime.strptime(string, format_code).isoformat()


@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#date_toDate",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    format_code="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_pattern",
)
def date_to_python_date(string, format_code):
    from datetime import datetime

    return datetime.strptime(string, format_code).isoformat()


@bif(fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_now")
def date_now():
    return datetime.now().isoformat()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_diff",
    date_1="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_datetime_d",
    date_2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_datetime_d2",
    unit="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_timeunit",
)
def date_diff(date_1: datetime, date_2: datetime, unit: str = None) -> str:
    if type(date_1) == str:
        date_1 = datetime.fromisoformat(date_1)
    date_1 = date_1.replace(tzinfo=None)
    if type(date_2) == str:
        date_2 = datetime.fromisoformat(date_2)
    date_2 = date_2.replace(tzinfo=None)
    timedelta = None
    if date_1 > date_2:
        timedelta = date_1 - date_2
    else:
        timedelta = date_2 - date_1
    if unit == "days" or unit == "d":
        return timedelta.days
    elif unit == "hours" or unit == "h":
        return timedelta.seconds * 60
    elif unit == "minutes" or unit == "m":
        return timedelta.days * 24 * 60 + timedelta.seconds // 60
    elif unit == "seconds" or unit == "s":
        return timedelta.seconds
    elif unit == "weeks" or unit == "w":
        return timedelta.days / 7

    return datetime.now().isoformat()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_datePart",
    date="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_date_d",
    unit="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_unit",
)
def date_diff(date, unit):
    if type(date) == str:
        date = datetime.fromisoformat(date)
    date = date.replace(tzinfo=None)
    if unit in ["years", "year"]:
        return date.year
    elif unit in ["months", "month"]:
        return date.month
    elif unit in ["weeks", "week", "w"]:
        return (date.day - 1) // 7 + 1
    elif unit in ["days", "day", "d"]:
        return date.day
    elif unit == "weekday":
        return date.strftime("%A")
    elif unit in ["hours", "hour", "h"]:
        return date.hour
    elif unit in ["minutes", "minute", "min"]:
        return date.minute
    elif unit in ["seconds", "sec", "s"]:
        return date.second
    elif unit in ["milliseconds", "ms", "S"]:
        return date.microsecond // 1000
    elif unit in ["nanos", "nano", "n"]:
        return date.microsecond * 1000
    elif unit == "time":
        epoch = datetime(1970, 1, 1)
        return int((date - epoch).total_seconds() * 1000)
    return datetime.now().isoformat()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_inc",
    date="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_date_d",
    inc="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
    unit="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_unit",
)
def date_diff(date, unit, inc):
    if type(date) == str:
        date = datetime.fromisoformat(date)
    inc = int(inc)
    date = date.replace(tzinfo=None)
    if unit in ["years", "year"]:
        return date.replace(year=date.year + inc)
    elif unit in ["months", "month"]:
        new_month = (date.month - 1 + inc) % 12 + 1
        new_year = date.year + (date.month - 1 + inc) // 12
        return date.replace(year=new_year, month=new_month)
    elif unit in ["weeks", "week", "w"]:
        return date + timedelta(weeks=inc)
    elif unit in ["days", "day", "d"]:
        return date + timedelta(days=inc)
    elif unit in ["hours", "hour", "h"]:
        return date + timedelta(hours=inc)
    elif unit in ["minutes", "minute", "min"]:
        return date + timedelta(minutes=inc)
    elif unit in ["seconds", "sec", "s"]:
        return date + timedelta(seconds=inc)
    elif unit in ["milliseconds", "ms", "S"]:
        return date + timedelta(milliseconds=inc)
    elif unit in ["nanos", "nano", "n"]:
        return date + timedelta(microseconds=inc // 1000)
    elif unit == "time":
        epoch = datetime(1970, 1, 1)
        return int((date - epoch).total_seconds() * 1000)
    return datetime.now().isoformat()


##############################################################################
########################   MATH   ############################################
##############################################################################


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_abs",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_abs(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    if value < 0:
        return -value
    return value


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_acos",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_acos(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.acos(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_sin",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_sin(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.sin(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_sinh",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_sinh(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.sinh(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_tan",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_tan(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.tan(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_tanh",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_tanh(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.tanh(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_asin",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_asin(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.asin(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_cos",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_cos(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.cos(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_cosh",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_cos(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.cosh(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_atan",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_atan(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.atan(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_atan2",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_n2",
)
def math_atan2(value, value2):
    value = literal_eval(value)
    value2 = literal_eval(value2)
    if (
            type(value) is not float
            and type(value) is not int
            and type(value2) is not float
            and type(value2) is not int
    ):
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.atan2(value, value2)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_ceil",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_ceil(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.ceil(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_combin",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i2",
)
def math_combin(value, value2):
    try:
        value = int(value)
        value2 = int(value2)
    except ValueError:
        raise ValueError("The value must be an int, not " + type(value).__name__)
    return math.comb(value, value2)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_degrees",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_atan(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.degrees(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_even",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_even(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.floor(value) % 2 == 0


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_odd",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_odd(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.floor(value) % 2 == 1


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_exp",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_exp(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.exp(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_fact",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i",
)
def math_fact(value):
    try:
        value = int(value)
    except:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.factorial(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_factn",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i2",
)
def math_factn(value, value2):
    try:
        value = int(value)
        value2 = int(value2)
    except:
        raise TypeError("The value must be a number, not " + type(value).__name__)

    return (
        math.prod(range(value2, value))
        if value > value2
        else math.prod(range(value, value2))
    )


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_gcd",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i2",
)
def math_gcd(value, value2):
    try:
        value = int(value)
        value2 = int(value2)
    except:
        raise TypeError("The value must be a number, not " + type(value).__name__)

    return math.gcd(value, value2)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_lcm",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i2",
)
def math_lcm(value, value2):
    try:
        value = int(value)
        value2 = int(value2)
    except:
        raise TypeError("The value must be a number, not " + type(value).__name__)

    return math.lcm(value, value2)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_ln",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_ln(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)

    return math.log(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_log",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def math_log(value):
    value = literal_eval(value)
    if type(value) is not float and type(value) is not int:
        raise TypeError("The value must be a number, not " + type(value).__name__)

    return math.log10(value)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_mod",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i2",
)
def math_mod(value, value2):
    try:
        value = int(value)
        value2 = int(value2)
    except:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return value % value2


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_multinomial",
    lst="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",
)
def math_multinomial(lst):
    if type(lst) != list:
        raise TypeError("The value must be a list, not " + type(lst).__name__)
    lst = [literal_eval(i) for i in lst]
    for i in lst:
        if type(i) is not int:
            raise TypeError("The value must be a number, not " + type(i).__name__)
    res, i = 1, sum(lst)
    i0 = lst.index(max(lst))
    for a in lst[:i0] + lst[i0 + 1:]:
        for j in range(1, a + 1):
            res *= i
            res //= j
            i -= 1
    return res


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_mod",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i2",
)
def math_mod(value, value2):
    try:
        value = int(value)
        value2 = int(value2)
    except:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return value % value2


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_pow",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_any_exp",
)
def math_pow(value, value2):
    value = literal_eval(value)
    value2 = literal_eval(value2)
    if (
            type(value) is not float
            and type(value) is not int
            and type(value2) is not float
            and type(value2) is not int
    ):
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.pow(value, value2)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_quotient",
    value="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
    value2="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_n2",
)
def math_quotient(value, value2):
    try:
        value = float(value)
        value2 = float(value2)

    except:
        raise TypeError("The value must be a number, not " + type(value).__name__)
    return math.floor(value / value2)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_max",
    number="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
    number2="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_n2",
)
def number_max(number, number2):
    number = literal_eval(number)
    number2 = literal_eval(number2)
    if (
            type(number) is not float
            and type(number) is not int
            and type(number2) is not float
            and type(number2) is not int
    ):
        raise TypeError("The value must be a number, not " + type(number).__name__)
    return number2 if number2 > number else number


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_min",
    number="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
    number2="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_n2",
)
def number_min(number, number2):
    number = literal_eval(number)
    number2 = literal_eval(number2)
    if (
            type(number) is not float
            and type(number) is not int
            and type(number2) is not float
            and type(number2) is not int
    ):
        raise TypeError("The value must be a number, not " + type(number).__name__)
    return number2 if number2 < number else number


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_floor",
    number="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def number_floor(number):
    number = literal_eval(number)
    if type(number) is not float and type(number) is not int:
        raise TypeError("The value must be a number, not " + type(number).__name__)
    return math.floor(number)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_radians",
    number="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
)
def number_radians(number):
    number = literal_eval(number)
    if type(number) is not float and type(number) is not int:
        raise TypeError("The value must be a number, not " + type(number).__name__)
    return math.radians(number)


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#math_randomNumber",
    start="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i",
    end="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_int_i2",
)
def number_radians(start="0", end="1"):
    try:
        start = float(start)
        end = float(end)
    except:
        raise TypeError("The value must be a number, not " + type(start).__name__)
    return random.uniform(start, end)


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
########################   STRING   ##########################################
##############################################################################


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
    return string if not string.startswith(diff_string) else string[len(diff_string):]


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
    split_2 = string[int(i_1): int(i_1) + int(i_2)]
    split_3 = string[int(i_1) + int(i_2): int(i_1) + int(i_2) + (int(p_rep_i))]
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
def string_partition(string: str, fragment: str, omit_fragment=False):
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
        string[0: len(string) - len(separator)]
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


##############################################################################
########################   OTHER   ###########################################
##############################################################################


# Hashs
@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#hash",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def hash(string):
    return hashlib.sha256(string.encode("UTF-8")).hexdigest()


@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#hash_iri",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def hash_iri(string):
    return f'http://example.com/ns#{hashlib.sha256(string.encode("UTF-8")).hexdigest()}'


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
     p_any_rep_element="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_any_rep_e", )
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