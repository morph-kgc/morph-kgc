""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


def merge_list_of_dicts(input_list):
    """
    Transform a list of dicts to a single dict. User must guarantee that keys of the dicts
    in the input list are unic. This function is intended for reconciling results
    after parallelization.
    :param input_list: list of dicts
    :type input_list: list
    :return: dict including key value pairs in the dicts of the input list
    :rtype dict
    """

    final_dict = {}
    for r in input_list:
        for key, value in r.items():
            final_dict[key] = value
    return final_dict
