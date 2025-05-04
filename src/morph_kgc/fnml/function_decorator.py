bif_dict = {}
def bif(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        bif_dict[fun_id] = {}
        bif_dict[fun_id]["function"] = funct
        bif_dict[fun_id]["parameters"] = params
        return funct

    return wrapper