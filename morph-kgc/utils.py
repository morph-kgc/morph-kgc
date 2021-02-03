""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


def get_repeated_elements_in_list(input_list):
    """
    Finds repeated elements in a list.

    :param input_list: list of elements
    :type input_list: list
    :return list with the repeated elements in input_list
    :rtype list
    """

    elem_count = {}
    for elem in input_list:
        if elem in elem_count:
            elem_count[elem] = elem_count[elem] + 1
        else:
            elem_count[elem] = 1

    repeated_elems = []
    for elem, elem_count in elem_count.items():
        if elem_count > 1:
            repeated_elems.append(elem)

    return repeated_elems
