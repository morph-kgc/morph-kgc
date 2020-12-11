import logging
import re


def get_references_in_template(template):
    template = template.replace('\{', 'zwy\u200B').replace('\}', 'ywz\u200A')

    references = re.findall('\{([^}]+)', template)
    references = [reference.replace('zwy\u200B', '\{').replace('ywz\u200A', '\}') for reference in references]

    return references