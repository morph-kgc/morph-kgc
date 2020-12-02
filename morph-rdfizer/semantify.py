""" Morph-RDFizer """

__version__ = "0.1"
__copyright__ = "Copyright (C) 2020 Julián Arenas Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "julian.arenas.guerrero@upm.es"


import logging
from mappings import parse_mappings
from args_parsing import parse_config
from configuration import configure_logger, get_configuration_and_sources


if __name__ == "__main__":

    config = parse_config()

    configure_logger(config)
    configuration, data_sources = get_configuration_and_sources(config)

    mappings_df = parse_mappings(data_sources, configuration)