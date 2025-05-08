from ..function_decorator import *

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

