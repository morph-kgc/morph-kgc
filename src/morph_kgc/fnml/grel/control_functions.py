from ..function_decorator import *
from functools import reduce
from operator import xor

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
def boolean_and(bool_input:list|str):
    if type(bool_input) == str:
        bool_input = [bool_input]
    return str(all([True if i.lower() in ["true", 1] else False for i in bool_input])).lower()

@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#boolean_or",
    bool_input="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_rep_b",
    
)
def boolean_and(bool_input:list|str):
    if type(bool_input) == str:
        bool_input = [bool_input]
    return str(any([True if i.lower() in ["true", 1] else False for i in bool_input])).lower()
@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#boolean_xor",
    bool_input="http://users.ugent.be/~bjdmeest/function/grel.ttl#param_rep_b",
    
)
def boolean_xor(bool_input:list|str):
    if type(bool_input) == str:
        bool_input = [bool_input]
    return str(reduce(xor, [True if i.lower() in ["true", 1] else False for i in bool_input])).lower()

@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#boolean_not",
    bool_input="http://users.ugent.be/~bjdmeest/function/grel.ttl#bool_b",
    
)
def boolean_not(bool_input):
    return str(not (True if bool_input.lower() in ["true", 1] else False)).lower()