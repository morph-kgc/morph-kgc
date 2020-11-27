import logging
from mappings import parse_mappings
from args_parsing import parse_config
from configuration import configure_logger, get_configuration_and_sources


if __name__ == "__main__":

    config = parse_config()

    configure_logger(config)
    configuration, data_sources = get_configuration_and_sources(config)

    mappings_df = parse_mappings(data_sources, configuration)

    mappings_df.sort_values(by='mapping_group', inplace=True, ascending=True)
