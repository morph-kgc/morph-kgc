import logging


def configure_logger(config, level=logging.INFO):
    """
    Configures the logger based on input arguments. If no logging argument is provided, log level is set to WARNING.

    :param config: the ConfigParser object
    :param level: logging level to use
    """

    if config.has_option('CONFIGURATION', 'logs'):
        '''do not include the date and time of an event'''
        # logging.basicConfig(filename=config.get('DEFAULT', 'logs'), filemode='w', level=level)

        '''include the date and time of an event'''
        logging.basicConfig(filename=config.get('CONFIGURATION', 'logs'),
                            format='%(levelname)s | %(asctime)s | %(message)s', filemode='w', level=level)
    else:
        logging.basicConfig(level=logging.WARNING)


def get_configuration_and_sources(config):
    """
    Separates the sources from the configuration options.

    :param config: ConfigParser object
    :return: tuple with the configuration options and the sources
    """

    configuration = dict(config.items('CONFIGURATION'))
    logging.info('CONFIGURATION: ' + str(configuration))

    data_sources = {}
    for section in config.sections():
        if section != 'CONFIGURATION':
            '''if section is not configuration then it is a data source'''
            '''mind that DEFAULT section is not triggered with config.sections()'''
            data_sources[section] = dict(config.items(section))

    logging.info('DATA SOURCES: ' + str(data_sources))

    return configuration, data_sources
