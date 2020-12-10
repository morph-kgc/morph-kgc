import logging, re


def get_configuration_and_sources(config):
    """
    Separates the sources from the configuration options.

    :param config: ConfigParser object
    :type config: configparser
    :return: tuple with the configuration options and the sources
    :rtype tuple
    """

    configuration = dict(config.items('CONFIGURATION'))
    logging.info('CONFIGURATION: ' + str(configuration))

    data_sources = {}
    for section in config.sections():
        if section != 'CONFIGURATION':
            ''' if section is not configuration then it is a data source.
                Mind that DEFAULT section is not triggered with config.sections(). '''
            data_sources[section] = dict(config.items(section))

    logging.info('DATA SOURCES: ' + str(data_sources))

    return configuration, data_sources


def get_references_in_template(template):
    template = template.replace('\{', 'zwy\u200B').replace('\}', 'ywz\u200A')

    references = re.findall('\{([^}]+)', template)
    references = [reference.replace('zwy\u200B', '\{').replace('ywz\u200A', '\}') for reference in references]

    return references