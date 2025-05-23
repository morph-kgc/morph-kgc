import random
from ..function_decorator import *
from ast import literal_eval
import math


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
    for a in lst[:i0] + lst[i0 + 1 :]:
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
