""" Morph-RDFizer """

__version__ = "0.1"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
from mapping_parser import parse_mappings
from args_parser import parse_config
from materializer import materialize
from morph_utils import get_configuration_and_sources


if __name__ == "__main__":

    config = parse_config()

    configuration, data_sources = get_configuration_and_sources(config)
    mappings_df = parse_mappings(data_sources, configuration)

    materialize(mappings_df, config)