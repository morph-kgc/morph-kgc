
from ..mapping.mapping_constants import FUNCTIONS_DATAFRAME_COLUMNS, FUNCTION_PARSING_QUERY



fno_imp_map = {
    "grel:toUpper": "to_upper",
    "grel:toLower": "to_lower",
}


def funcs_to_df(g):
    function_query_results = g.query(FUNCTION_PARSING_QUERY)

    # DEBUG
    print("\nXXXXXXX\nfuncs_to_df\nresults:")
    for res_bindings in function_query_results.bindings:
        print("-------")
        for k in res_bindings:
            print("%s: %s" % (k, res_bindings[k]))
        # print("exec[%s] -- func[%s] -- parameter_uri[%s] -- func1[%s]" % (res["exec"], res["func"], res["parameter_uri"], res["func1"]))
        # print("exec [%s]" % (res["exec"]))
        # print("func [%s]" % (res["func"]))
        # # print("func1 [%s]" % (res["func1"]))
        # print("parameter_uri [%s]" % (res["parameter_uri"]))
        # print("p [%s]" % (res["p"]))
        # print("o [%s]" % (res["o"]))
        # print(res)

