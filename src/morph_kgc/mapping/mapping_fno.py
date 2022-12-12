
from ..mapping.mapping_constants import FUNCTIONS_DATAFRAME_COLUMNS, FUNCTION_PARSING_QUERY



fno_imp_map = {
    "grel:toUpper": "to_upper",
    "grel:toLower": "to_lower",
}


def funcs_to_df(g):
    function_query_results = g.query(FUNCTION_PARSING_QUERY)

    # DEBUG
    print("\nXXXXXXX\n\n")
    for res in function_query_results:
        print(res)

