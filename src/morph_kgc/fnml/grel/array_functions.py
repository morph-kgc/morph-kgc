from ..function_decorator import *
from ast import literal_eval


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
def array_join(array:list|str, p_string_sep):
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
def array_reverse(array:list):
    # it does not explode
    if type(array) != list:
        return array
    return array[::-1]
@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_uniques",
    array="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",   
)
def array_uniques(array:list):
    return [x for i, x in enumerate(array) if x not in array[:i]]

@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#array_sort",
    array="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_array_a",   
)
def array_sort(array:list):
    if type(array) != list:
        return None
    array.sort()
    return array