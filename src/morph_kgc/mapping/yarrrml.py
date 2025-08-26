__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import rdflib

from ruamel.yaml import YAML
from copy import deepcopy
from random import randint
import re

from ..constants import *


# dictionary mapping reference formulations in YARRRML to RML
REFERENCE_FORMULATION_DICT = {
    'csv': RML_CSV,
    'jsonpath': RML_JSONPATH,
    'xpath': RML_XPATH,
    'cypher': RML_CYPHER,
    'sql2008': RML_SQL2008
}


def _template_to_rml(yarrrml_template):
    rml_template = ''

    ref_ini_pos = yarrrml_template.find('$(')
    while ref_ini_pos != -1:
        rml_template += f'{yarrrml_template[:ref_ini_pos]}{{'
        yarrrml_template = f'{yarrrml_template[ref_ini_pos+2:]}'

        ref_end_pos = yarrrml_template.find(')')
        rml_template += f'{yarrrml_template[:ref_end_pos]}}}'
        yarrrml_template = yarrrml_template[ref_end_pos+1:]

        ref_ini_pos = yarrrml_template.find('$(')

    # final constant string
    rml_template += yarrrml_template

    return rml_template


def _add_source(mapping_graph, source, source_bnode):
    if 'access' in source:
        mapping_graph.add((source_bnode, rdflib.term.URIRef(RML_SOURCE), rdflib.term.Literal(source['access'])))
    if 'query' in source:
        mapping_graph.add((source_bnode, rdflib.term.URIRef(RML_QUERY), rdflib.term.Literal(source['query'])))
    if 'table' in source:
        mapping_graph.add((source_bnode, rdflib.term.URIRef(RML_TABLE_NAME), rdflib.term.Literal(source['table'])))
    if 'iterator' in source:
        mapping_graph.add((source_bnode, rdflib.term.URIRef(RML_ITERATOR), rdflib.term.Literal(source['iterator'])))
    if 'referenceFormulation' in source:
        mapping_graph.add((source_bnode, rdflib.term.URIRef(RML_REFERENCE_FORMULATION),
                           rdflib.term.Literal(REFERENCE_FORMULATION_DICT[source['referenceFormulation'].lower()])))

    return mapping_graph


def _add_template(mapping_graph, term_map_bnode, yarrrml_template):
    yarrrml_template = str(yarrrml_template)
    if yarrrml_template.startswith('$(') and yarrrml_template.count('$(') == 1:
        # a YARRRML template may be composed of simply one reference
        # in that case the YARRRML template corresponds to an RML reference
        mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_REFERENCE), rdflib.term.Literal(yarrrml_template[2:-1])))
    elif '$(' in yarrrml_template:
        rml_template = _template_to_rml(yarrrml_template)
        mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_TEMPLATE), rdflib.term.Literal(rml_template)))
    elif 'a' == yarrrml_template:
        mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(RDF_TYPE)))
    else:
        # a YARRRML template may have 0 references
        # in that case the YARRRML template corresponds to an RML constant
        if yarrrml_template.startswith('http') or yarrrml_template.startswith('ftp'):
            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(yarrrml_template)))
        else:
            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.Literal(yarrrml_template)))

    return mapping_graph


def _normalize_yarrrml_key_names(mappings):
    if type(mappings) is dict:
        for key, value in mappings.copy().items():
            if key in ['mapping', 'm']:
                mappings['mappings'] = mappings.pop(key)
            elif key in ['subject', 's']:
                mappings['subjects'] = mappings.pop(key)
            elif key in ['predicateobject', 'po']:
                mappings['predicateobjects'] = mappings.pop(key)
            elif key in ['predicate', 'p']:
                mappings['predicates'] = mappings.pop(key)
            elif key in ['inversepredicate', 'i']:
                mappings['inversepredicates'] = mappings.pop(key)
            elif key in ['object', 'o']:
                mappings['objects'] = mappings.pop(key)
            elif key in ['graph', 'g']:
                mappings['graphs'] = mappings.pop(key)
            elif key in ['fn', 'f']:
                mappings['function'] = mappings.pop(key)
            elif key in ['pms']:
                mappings['parameters'] = mappings.pop(key)
            elif key in ['pm']:
                mappings['parameter'] = mappings.pop(key)
            elif key in ['v']:
                mappings['value'] = mappings.pop(key)
            elif key in ['author', 'a']:
                mappings['authors'] = mappings.pop(key)

        for key, value in mappings.items():
            mappings[key] = _normalize_yarrrml_key_names(value)

    elif type(mappings) is list:
        for i, value in enumerate(mappings):
            mappings[i] = _normalize_yarrrml_key_names(value)

    return mappings


def _add_default_prefixes(mappings):
    default_prefixes = {
        'rml': RML_NAMESPACE,
        'fno': FNO_NAMESPACE,
        'xsd': XSD_NAMESPACE,
        'rdfs': RDFS_NAMESPACE,
        'as': 'https://www.w3.org/ns/activitystreams#',
        'csvw': 'http://www.w3.org/ns/csvw#',
        'dcat': 'http://www.w3.org/ns/dcat#',
        'dqv': 'http://www.w3.org/ns/dqv#',
        'duv': 'https://www.w3.org/ns/duv#',
        'grddl': 'http://www.w3.org/2003/g/data-view#',
        'jsonld': 'http://www.w3.org/ns/json-ld#',
        'ldp': 'http://www.w3.org/ns/ldp#',
        'ma': 'http://www.w3.org/ns/ma-ont#',
        'oa': 'http://www.w3.org/ns/oa#',
        'odrl': 'http://www.w3.org/ns/odrl/2/',
        'org': 'http://www.w3.org/ns/org#',
        'owl': 'http://www.w3.org/2002/07/owl#',
        'prov': 'http://www.w3.org/ns/prov#',
        'qb': 'http://purl.org/linked-data/cube#',
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfa': 'http://www.w3.org/ns/rdfa#',
        'rif': 'http://www.w3.org/2007/rif#',
        'rr': 'http://www.w3.org/ns/r2rml#',
        'sd': 'http://www.w3.org/ns/sparql-service-description#',
        'skos': 'http://www.w3.org/2004/02/skos/core#',
        'skosxl': 'http://www.w3.org/2008/05/skos-xl#',
        'ssn': 'http://www.w3.org/ns/ssn/',
        'sosa': 'http://www.w3.org/ns/sosa/',
        'time': 'http://www.w3.org/2006/time#',
        'void': 'http://rdfs.org/ns/void#',
        'wdr': 'http://www.w3.org/2007/05/powder#',
        'wdrs': 'http://www.w3.org/2007/05/powder-s#',
        'xhv': 'http://www.w3.org/1999/xhtml/vocab#',
        'xml': 'http://www.w3.org/XML/1998/namespace',
        'cc': 'http://creativecommons.org/ns#',
        'ctag': 'http://commontag.org/ns#',
        'dc': 'http://purl.org/dc/terms/',
        'dcterms': 'http://purl.org/dc/terms/',
        'dc11': 'http://purl.org/dc/elements/1.1/',
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'gr': 'http://purl.org/goodrelations/v1#',
        'ical': 'http://www.w3.org/2002/12/cal/icaltzd#',
        'og': 'http://ogp.me/ns#',
        'rev': 'http://purl.org/stuff/rev#',
        'sioc': 'http://rdfs.org/sioc/ns#',
        'v': 'http://rdf.data-vocabulary.org/#',
        'vcard': 'http://www.w3.org/2006/vcard/ns#',
        'schema': 'http://schema.org/'
    }
    if 'prefixes' in mappings:
        mappings['prefixes'].update(default_prefixes)
    else:
        mappings['prefixes'] = default_prefixes

    return mappings


def _replace_yarrrml_external_references(mappings, external_references):
    if type(mappings) is dict:
        for key, value in mappings.items():
            mappings[key] = _replace_yarrrml_external_references(value, external_references)
    elif type(mappings) is list:
        for i, value in enumerate(mappings):
            mappings[i] = _replace_yarrrml_external_references(value, external_references)
    elif type(mappings) is str:
        for external_references_key, external_references_value in external_references.items():
            if mappings == f'$(_{external_references_key})':
                mappings = external_references_value
            elif mappings == f'$(\\_{external_references_key})':
                # comply with example 110 in YARRRML spec
                mappings = f'$(_{external_references_key})'

    return mappings


def _expand_prefixes_in_yarrrml_templates(mappings, prefixes):
    if type(mappings) is dict:
        for key, value in mappings.items():
            mappings[key] = _expand_prefixes_in_yarrrml_templates(value, prefixes)
    elif type(mappings) is list:
        for i, value in enumerate(mappings):
            mappings[i] = _expand_prefixes_in_yarrrml_templates(value, prefixes)
    elif type(mappings) is str:
        for prefix_key, prefix_value in prefixes.items():
            if mappings.startswith(f'{prefix_key}:'):
                mappings = mappings.replace(f'{prefix_key}:', prefix_value)

    return mappings


def _expand_source_shortcut(source_value):
    if type(source_value) is list:
        if '~' in source_value[0]:
            access, reference_formulation = source_value[0].split('~')
            source_value_dict = {'access': access, 'referenceFormulation': reference_formulation}
        else:
            source_value_dict = {'access': source_value[0]}

        if len(source_value) == 2:
            source_value_dict['iterator'] = source_value[1]

        return source_value_dict
    return source_value


def _normalize_property_in_mapping(mappings, property):
    # expand list of the property (e.g. sources, subjects) inside a mapping with independent mappings for each property
    for mapping_key, mapping_value in mappings['mappings'].copy().items():
        if property in mapping_value and type(mapping_value[property]) is list:
            for i, property_value in enumerate(mapping_value[property]):
                aux_mapping_value = mapping_value.copy()
                aux_mapping_value[property] = property_value
                mappings['mappings'][f'{mapping_key}_-_-_{property}{i}'] = aux_mapping_value
            mappings['mappings'].pop(mapping_key)

    return mappings


def _normalize_property_in_predicateobjects(mappings, property):
    # expand list of the property (e.g. predicate, objects, graphs) inside a preicateobject with independent mappings for each property
    for mapping_key, mapping_value in mappings['mappings'].copy().items():
        if 'predicateobjects' in mapping_value and property in mapping_value['predicateobjects']:
            if type(mapping_value['predicateobjects'][property]) is list:
                aux_mapping_value = mapping_value.copy()
                for i, property_value in enumerate(mapping_value['predicateobjects'][property]):
                    aux_mapping_value['predicateobjects'][property] = property_value
                    mappings['mappings'][f'{mapping_key}_-_-_{property}{i}'] = deepcopy(aux_mapping_value)
                mappings['mappings'].pop(mapping_key)

    return mappings


def _normalize_conditional_mappings(mappings: dict):
    """
    Recursively normalizes conditional mappings in a nested dictionary structure.
    This function processes a dictionary of mappings and modifies it in-place to handle
    conditional creates. If a mapping contains a "condition" key with a "function" that
    is not "equal" (not a join), it transforms the mapping by replacing the condition with a new
    structure that includes a "trueCondition" function and its associated parameters.
    That ensures that the mappings are only created if the conditions are met.
    The function also ensures that nested dictionaries are processed recursively.
    Args:
        mappings (dict): A dictionary containing the mappings to be normalized.
    Returns:
        dict: The normalized dictionary with updated conditional mappings.
    """
    keys_to_iterate = [yaml_key for yaml_key in mappings]
    for yaml_key in keys_to_iterate:
        if yaml_key != "condition" and type(mappings[yaml_key]) == dict:
            mappings[yaml_key] = _normalize_conditional_mappings(mappings[yaml_key])
        if "condition" in mappings:
            if (type(mappings["condition"]) == list):
                mappings["condition"] = mappings["condition"][0]
            if (
                "function" in mappings["condition"]
                and mappings["condition"]["function"] != "equal"
            ):
                condition_function = {
                    "objects": {
                        "type": (
                            mappings["objects"]["type"]
                            if "type" in mappings["objects"]
                            else "literal"
                        ),
                        
                            "function": "https://w3id.org/imec/idlab/function#trueCondition",
                            "parameters": [
                                {
                                    "parameter": "https://w3id.org/imec/idlab/function#str",
                                    "value": mappings["objects"]["value"],
                                },
                                {
                                    "parameter": "https://w3id.org/imec/idlab/function#strBoolean",
                                    "value": mappings["condition"],
                                },
                            ],
                        
                    }
                }
                mappings.update(condition_function)
                mappings.pop("condition")

    return mappings


def _normalize_function_parameters(term_map, prefixes):
    if type(term_map) is dict and 'parameters' in term_map:
        if type(term_map['parameters']) is list:
            for i, parameter in enumerate(term_map['parameters']):
                if type(parameter) is list:
                    term_map['parameters'][i] = {'parameter': parameter[0], 'value': parameter[1]}
                elif type(parameter) is dict:
                    term_map['parameters'][i] = parameter

                if type(term_map['parameters'][i]['value']) is dict and 'function' in term_map['parameters'][i]['value']:
                    term_map['parameters'][i]['parameter'] = term_map['parameters'][i]['parameter']
                    # term_map['parameters'][i]['value'] = _normalize_function_parameters(term_map['parameters'][i]['value'], prefixes)
    elif type(term_map) is dict and 'function' in term_map and term_map['function'].endswith(')'):
        # inline function examples 99 & 101 YARRRML spec
        inline_function = term_map['function']
        function_id = inline_function.split('(')[0]
        # get the parameters by removing the function id, the parenthesis, and whitespaces
        inline_inputs = inline_function.replace(function_id, '')[1:-1]
        inline_inputs = re.sub(r"((\s))|((' ')|(\" \"))", r"\3", inline_inputs)

        inline_parameters_dict = {}
        for input in inline_inputs.split(','):
            input_parameter, input_value = input.split('=')
            if input_value.startswith("\"") and input_value.endswith("\"") or \
                input_value.startswith("'") and input_value.endswith("'"):
                # remove the quotes from the value
                input_value = input_value[1:-1]
            if not input_parameter.startswith('http') and ':' not in input_parameter:
                # the prefix of the parameter is the same as the prefix of the function
                included_prefixes = list(prefixes.values())
                for included_prefix in included_prefixes:
                    if function_id.startswith(included_prefix):
                        input_parameter = included_prefix + input_parameter
                        break              
            inline_parameters_dict[input_parameter] = input_value

        # final normalized term map
        term_map = {'function': function_id}
        for input_parameter, input_value in inline_parameters_dict.items():
            if 'parameters' not in term_map:
                term_map['parameters'] = []
            term_map['parameters'].append({'parameter': input_parameter, 'value': input_value})

    return term_map


def _normalize_yarrrml_mapping(mappings, prefixes):

    #############################################################################
    ############################ NORMALIZE SOURCES ##############################
    #############################################################################

    # expand sources outside mapping
    if 'sources' in mappings:
        for source_key in mappings['sources']:
            mappings['sources'][source_key] = _expand_source_shortcut(mappings['sources'][source_key])

    # expand sources inside mapping
    for mapping_key, mapping_value in mappings['mappings'].items():
        if type(mapping_value['sources']) is list:
            for i, source in enumerate(mapping_value['sources']):
                mapping_value['sources'][i] = _expand_source_shortcut(source)

    # replace sources references with actual sources inside the mapping
    if 'sources' in mappings:
        for mapping_key, mapping_value in mappings['mappings'].items():
            if type(mapping_value['sources']) is str:
                mappings['mappings'][mapping_key]['sources'] = mappings['sources'][mapping_value['sources']]
            elif type(mapping_value['sources']) is list:
                for i, source in enumerate(mapping_value['sources']):
                    if type(source) is str:
                        mappings['mappings'][mapping_key]['sources'][i] = mappings['sources'][source]
        mappings.pop('sources')


    #############################################################################
    ############################ NORMALIZE TRIPLES MAPS #########################
    #############################################################################

    for property in ['sources', 'subjects', 'predicateobjects']:
        mappings = _normalize_property_in_mapping(mappings, property)

    # predicateobject shortcuts [foaf: firstName, $(firstname)] to dict
    #- [foaf: firstName, $(firstname), xsd: string]
    #- [[foaf: knows, rdfs: label], $(colleague)~iri]
    #- [[foaf: name, rdfs: label], [$(firstname), $(lastname)]]
    #- [foaf: firstName, $(firstname), en~lang]
    for mapping_key, mapping_value in mappings['mappings'].items():
        if 'predicateobjects' in mapping_value:
            predicateobject = mapping_value['predicateobjects']
            if type(predicateobject) is list:
                if len(predicateobject) == 2:
                    predicates, objects = predicateobject
                    predicateobject_dict = {'predicates': predicates, 'objects': objects}
                else:
                    predicates, objects, lang_datatype = predicateobject
                    predicateobject_dict = {'predicates': predicates, 'objects': {'value': objects}}
                    if lang_datatype.endswith('~lang'):
                        predicateobject_dict['objects']['language'] = lang_datatype[:-5]
                    else:
                        predicateobject_dict['objects']['datatype'] = lang_datatype
                mapping_value['predicateobjects'] = predicateobject_dict

    # move graphs in subjects to predicateobjects
    for mapping_key, mapping_value in mappings['mappings'].items():
        if 'graphs' in mapping_value and 'predicateobjects' in mapping_value:
            if 'graphs' in mapping_value['predicateobjects']:
                # there are graphs in the subjects and the predicateobjects
                graphs_subject_list = mapping_value['graphs'] if type(mapping_value['graphs']) is list else [
                    mapping_value['graphs']]
                mapping_value['predicateobjects']['graphs'] = mapping_value['predicateobjects']['graphs'] if type(
                    mapping_value['predicateobjects']['graphs']) is list else [
                    mapping_value['predicateobjects']['graphs']]
                mapping_value['predicateobjects']['graphs'].extend(graphs_subject_list)
            else:
                mapping_value['predicateobjects']['graphs'] = mapping_value['graphs']
            mapping_value.pop('graphs')

    # expand objects: [[$(firstname), en~lang], [$(lastname), nl~lang]] (Example 83 in YARRRML spec)
    for mapping_key, mapping_value in mappings['mappings'].items():
        if 'predicateobjects' in mapping_value and 'objects' in mapping_value['predicateobjects'] and type(mapping_value['predicateobjects']['objects']) is list:
            if type(mapping_value['predicateobjects']['objects'][0]) is list:
                for i, object in enumerate(mapping_value['predicateobjects']['objects']):
                    value, lang_datatype = object
                    if '~' in lang_datatype:
                        mapping_value['predicateobjects']['objects'][i] = {'value': value, 'language': lang_datatype[:-5]}
                    else:
                        mapping_value['predicateobjects']['objects'][i] = {'value': value, 'datatype': lang_datatype}

    # lists of predicates, objects and graphs to independent mappings
    for property in ['predicates', 'objects', 'graphs']:
        mappings = _normalize_property_in_predicateobjects(mappings, property)

    # create `value` in objects and expand ~iri ~blanknode ~literal
    for mapping_key, mapping_value in mappings['mappings'].items():
        if 'predicateobjects' in mapping_value:
            if 'subjects' in mapping_value:
                if type(mapping_value['subjects']) is str:
                    if mapping_value['subjects'].endswith(('~iri', '~blanknode')):
                        value, termtype = mapping_value['subjects'].split('~')
                        mapping_value['subjects'] = {'value': value, 'type': termtype}
                    else:
                        mapping_value['subjects'] = {'value': mapping_value['subjects']}
            if 'objects' in mapping_value['predicateobjects']:
                if type(mapping_value['predicateobjects']['objects']) is str:
                    if mapping_value['predicateobjects']['objects'].endswith(('~iri', '~literal', '~blanknode')):
                        value, termtype = mapping_value['predicateobjects']['objects'].split('~')
                        mapping_value['predicateobjects']['objects'] = {'value': value, 'type': termtype}
                    else:
                        mapping_value['predicateobjects']['objects'] = {'value': mapping_value['predicateobjects']['objects']}

            # type, datatype, language defined in the predicateobject level (example 68 YARRRML spec)
            for property in ['type', 'datatype', 'language']:
                if property in mapping_value['predicateobjects']:
                    mapping_value['predicateobjects']['objects'][property] = mapping_value['predicateobjects'][property]

    #############################################################################
    ############################ FUNCTIONS ######################################
    #############################################################################
    mappings = _normalize_conditional_mappings(mappings)
    for mapping_key, mapping_value in mappings['mappings'].items():
        if 'subjects' in mapping_value and type(mapping_value['subjects']) is dict and 'function' in mapping_value['subjects']:
            mapping_value['subjects'] = _normalize_function_parameters(mapping_value['subjects'], prefixes)
        if type(mapping_value) is dict and 'predicateobjects' in mapping_value:
            for position in ['predicates', 'objects', 'graphs']:
                if position in mapping_value['predicateobjects'] and 'function' in mapping_value['predicateobjects'][position]:
                    mapping_value['predicateobjects'][position].update(_normalize_function_parameters(mapping_value['predicateobjects'][position], prefixes))

    #############################################################################
    ############################ INVERSE PREDICATES #############################
    #############################################################################

    for mapping_key, mapping_value in mappings['mappings'].copy().items():
        if 'predicateobjects' in mapping_value:
            if 'inversepredicates' in mapping_value['predicateobjects']:
                # if inversepredicates is not a list make it a list of one element to simplify processing
                if type(mapping_value['predicateobjects']['inversepredicates']) is list:
                    inverse_predicates = mapping_value['predicateobjects']['inversepredicates']
                else:
                    inverse_predicates = [mapping_value['predicateobjects']['inversepredicates']]
                mapping_value['predicateobjects'].pop('inversepredicates')

                for inverse_predicate in inverse_predicates:
                    inverse_mapping_value = mapping_value.copy()
                    inverse_mapping_value['subjects'] = mapping_value['predicateobjects']['objects']
                    inverse_mapping_value['predicateobjects'] = {}
                    inverse_mapping_value['predicateobjects']['predicates'] = inverse_predicate
                    inverse_mapping_value['predicateobjects']['objects'] = mapping_value['subjects']
                    mappings['mappings'][f'{mapping_key}_inverse{randint(0,1000000)}'] = inverse_mapping_value

    return mappings


def _translate_yarrrml_function_to_rml(mapping_graph, function, term_map):
    execution_bnode = rdflib.term.BNode()
    mapping_graph.add((term_map, rdflib.term.URIRef(RML_EXECUTION), execution_bnode))

    if 'datatype' in function:
        mapping_graph.add((term_map, rdflib.term.URIRef(RML_DATATYPE_SHORTCUT), rdflib.term.URIRef(function['datatype'])))
    elif 'language' in function:
        mapping_graph.add((term_map, rdflib.term.URIRef(RML_LANGUAGE_SHORTCUT), rdflib.term.URIRef(function['language'])))
    elif 'type' in function:
        if function['type'] == 'iri':
            mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_IRI)))
        elif function['type'] == 'literal':
            mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_LITERAL)))
        elif function['type'] == 'blanknode':
            mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_BLANK_NODE)))
        else:
            raise ValueError(f"Found an invalid termtype `{function['type']}` in YARRRML mapping.")

    function_bnode = rdflib.term.BNode()
    mapping_graph.add((execution_bnode, rdflib.term.URIRef(RML_FUNCTION_MAP), function_bnode))
    mapping_graph.add((function_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(function['function'])))

    if 'parameters' in function:
        # TODO: deal with recursivity
        for i, parameter in enumerate(function['parameters']):
            input_bnode = rdflib.term.BNode()
            mapping_graph.add((execution_bnode, rdflib.term.URIRef(RML_INPUT), input_bnode))

            parameter_bnode = rdflib.term.BNode()
            mapping_graph.add((input_bnode, rdflib.term.URIRef(RML_PARAMETER_MAP), parameter_bnode))

            value_bnode = rdflib.term.BNode()
            mapping_graph.add((input_bnode, rdflib.term.URIRef(RML_VALUE_MAP), value_bnode))

            if type(parameter['value']) is dict and 'function' in parameter['value']:
                # composite function
                mapping_graph.add((parameter_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(parameter['parameter'])))
                mapping_graph = _translate_yarrrml_function_to_rml(mapping_graph, parameter['value'], value_bnode)
            else:
                mapping_graph.add((parameter_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(parameter['parameter'])))
                mapping_graph = _add_template(mapping_graph, value_bnode, parameter['value'])

    return mapping_graph


def _translate_yarrrml_to_rml(yarrrml_mapping):
    tm_id_to_norm_tm_ids = {}
    for mapping_id, mapping_value in yarrrml_mapping['mappings'].items():
        if '_-_-_' in mapping_id:
            orig_mapping_id = mapping_id.split('_-_-_')[0]
        else:
            orig_mapping_id = mapping_id

        if orig_mapping_id in tm_id_to_norm_tm_ids:
            tm_id_to_norm_tm_ids[orig_mapping_id].add(mapping_id)
        else:
            tm_id_to_norm_tm_ids[orig_mapping_id] = {mapping_id}

    mapping_graph = rdflib.Graph()

    ########################################################
    ####################### PREFIXES #######################
    ########################################################

    prefixes_dict = yarrrml_mapping['prefixes'] if 'prefixes' in yarrrml_mapping else {}
    for prefix_key, prefix_value in prefixes_dict.items():
        mapping_graph.bind(prefix_key, rdflib.term.URIRef(prefix_value))


    ########################################################
    ####################### MAPPINGS #######################
    ########################################################

    for mapping_id, mapping_value in yarrrml_mapping['mappings'].items():
        triples_map_iri = rdflib.term.URIRef(mapping_id)

        ####################### SOURCES #####################
        source_bnode = rdflib.BNode()
        mapping_graph.add((triples_map_iri, rdflib.term.URIRef(RML_LOGICAL_SOURCE), source_bnode))
        mapping_graph = _add_source(mapping_graph, mapping_value['sources'], source_bnode)

        ####################### SUBJECTS ####################
        if 'subjects' in mapping_value:
            subject_bnode = rdflib.BNode()
            mapping_graph.add((triples_map_iri, rdflib.term.URIRef(RML_SUBJECT_MAP), subject_bnode))
            if type(mapping_value['subjects']) is str:
                mapping_graph = _add_template(mapping_graph, subject_bnode, mapping_value['subjects'])
            elif type(mapping_value['subjects']) is dict:
                # it is quoted
                if 'quoted' in mapping_value['subjects']:
                    for ref_tm in tm_id_to_norm_tm_ids[mapping_value['subjects']['quoted']]:
                        mapping_graph.add((subject_bnode, rdflib.term.URIRef(RML_QUOTED_TRIPLES_MAP), rdflib.term.URIRef(ref_tm)))
                elif 'quotedNonAsserted' in mapping_value['subjects']:
                    # only non asserted triples maps are typed
                    for ref_tm in tm_id_to_norm_tm_ids[mapping_value['subjects']['quotedNonAsserted']]:
                        mapping_graph.add((subject_bnode, rdflib.term.URIRef(RML_QUOTED_TRIPLES_MAP), rdflib.term.URIRef(ref_tm)))
                        mapping_graph.add((rdflib.term.URIRef(ref_tm), rdflib.term.URIRef(RDF_TYPE), rdflib.term.URIRef(RML_NON_ASSERTED_TRIPLES_MAP_CLASS)))
                elif 'function' in mapping_value['subjects']:
                    mapping_graph = _translate_yarrrml_function_to_rml(mapping_graph, mapping_value['subjects'], subject_bnode)
                else:
                    mapping_graph = _add_template(mapping_graph, subject_bnode, mapping_value['subjects']['value'])

                if 'condition' in mapping_value['subjects']:
                    join_condition_bnode = rdflib.BNode()
                    mapping_graph.add((subject_bnode, rdflib.term.URIRef(RML_JOIN_CONDITION), join_condition_bnode))
                    for parameter in mapping_value['subjects']['condition']['parameters']:
                        if parameter[0] == 'str1':
                            mapping_graph.add((join_condition_bnode, rdflib.term.URIRef(RML_CHILD), rdflib.term.Literal(parameter[1][2:-1])))
                        elif parameter[0] == 'str2':
                            mapping_graph.add((join_condition_bnode, rdflib.term.URIRef(RML_PARENT), rdflib.term.Literal(parameter[1][2:-1])))
                if 'type' in mapping_value['subjects']:
                    if mapping_value['subjects']['type'] == 'iri':
                        mapping_graph.add((subject_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_IRI)))
                    elif mapping_value['subjects']['type'] == 'blanknode':
                        mapping_graph.add((subject_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_BLANK_NODE)))
                    else:
                        raise ValueError(f"Found an invalid termtype `{mapping_value['subjects']['type']}` in YARRRML mapping.")
        else:
            # it is a blank node
            subject_bnode = rdflib.BNode()
            mapping_graph.add((triples_map_iri, rdflib.term.URIRef(RML_SUBJECT_MAP), subject_bnode))
            mapping_graph.add((subject_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.BNode()))
            mapping_graph.add((subject_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_BLANK_NODE)))

        ####################### GRAPHS ####################
        if 'graphs' in mapping_value:
            graph_bnode = rdflib.BNode()
            mapping_graph.add((triples_map_iri, rdflib.term.URIRef(RML_GRAPH_MAP), graph_bnode))
            mapping_graph = _add_template(mapping_graph, graph_bnode, mapping_value['graphs'])

        ####################### PREDICATE OBJECTS ############
        if 'predicateobjects' in mapping_value:
            predicateobject_bnode = rdflib.BNode()
            mapping_graph.add((triples_map_iri, rdflib.term.URIRef(RML_PREDICATE_OBJECT_MAP), predicateobject_bnode))

            for position, property in zip(['predicates', 'objects', 'graphs'], [RML_PREDICATE_MAP, RML_OBJECT_MAP, RML_GRAPH_MAP]):
                if position in mapping_value['predicateobjects']:
                    term_map_bnode = rdflib.BNode()
                    if type(mapping_value['predicateobjects'][position]) is str:
                        # template
                        mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                        mapping_graph = _add_template(mapping_graph, term_map_bnode, mapping_value['predicateobjects'][position])
                    elif type(mapping_value['predicateobjects'][position]) is dict:
                        if 'function' in mapping_value['predicateobjects'][position]:
                            mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                            mapping_graph = _translate_yarrrml_function_to_rml(mapping_graph, mapping_value['predicateobjects'][position], term_map_bnode)
                        elif 'mappings' in mapping_value['predicateobjects'][position]:
                            # referencing object map

                            # just a single normalized triples map is needed (only the subject map is used)
                            ref_tm = list(tm_id_to_norm_tm_ids[mapping_value['predicateobjects'][position]['mappings']])[0]

                            mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_PARENT_TRIPLES_MAP), rdflib.term.URIRef(ref_tm)))
                        elif 'quoted' in mapping_value['predicateobjects'][position] or 'quotedNonAsserted' in mapping_value['predicateobjects'][position]:
                            mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                            if 'quoted' in mapping_value['predicateobjects'][position]:
                                for ref_tm in tm_id_to_norm_tm_ids[mapping_value['predicateobjects'][position]['quoted']]:
                                    mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_QUOTED_TRIPLES_MAP), rdflib.term.URIRef(ref_tm)))
                            elif 'quotedNonAsserted' in mapping_value['predicateobjects'][position]:
                                # only non asserted triples maps are typed
                                for ref_tm in tm_id_to_norm_tm_ids[mapping_value['predicateobjects'][position]['quotedNonAsserted']]:
                                    mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_QUOTED_TRIPLES_MAP), rdflib.term.URIRef(ref_tm)))
                                    mapping_graph.add((rdflib.term.URIRef(ref_tm), rdflib.term.URIRef(RDF_TYPE), rdflib.term.URIRef(RML_NON_ASSERTED_TRIPLES_MAP_CLASS)))
                        else:
                            # object dict
                            mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                            mapping_graph = _add_template(mapping_graph, term_map_bnode, mapping_value['predicateobjects'][position]['value'])

                        if 'condition' in mapping_value['predicateobjects'][position]:
                            join_condition_bnode = rdflib.BNode()
                            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_JOIN_CONDITION), join_condition_bnode))
                            for parameter in mapping_value['predicateobjects'][position]['condition']['parameters']:
                                if parameter[0] == 'str1':
                                    mapping_graph.add((join_condition_bnode, rdflib.term.URIRef(RML_CHILD), rdflib.term.Literal(parameter[1][2:-1])))
                                elif parameter[0] == 'str2':
                                    mapping_graph.add((join_condition_bnode, rdflib.term.URIRef(RML_PARENT), rdflib.term.Literal(parameter[1][2:-1])))
                        if 'language' in mapping_value['predicateobjects'][position]:
                            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_LANGUAGE_SHORTCUT), rdflib.term.Literal(mapping_value['predicateobjects'][position]['language'])))
                        elif 'datatype' in mapping_value['predicateobjects'][position]:
                            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_DATATYPE_SHORTCUT), rdflib.term.URIRef(mapping_value['predicateobjects'][position]['datatype'])))
                        elif 'type' in mapping_value['predicateobjects'][position]:
                            if mapping_value['predicateobjects'][position]['type'] == 'iri':
                                mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_IRI)))
                            elif mapping_value['predicateobjects'][position]['type'] == 'literal':
                                mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_LITERAL)))
                            elif mapping_value['predicateobjects'][position]['type'] == 'blanknode':
                                mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_BLANK_NODE)))
                            else:
                                raise ValueError(f"Found an invalid termtype `{mapping_value['predicateobjects'][position]['type']}` in YARRRML mapping.")

    return mapping_graph


def load_yarrrml(yarrrml_file):
    with open(yarrrml_file) as f:
        yaml = YAML(typ='safe', pure=True)
        yarrrml_mapping = yaml.load(f)

    yarrrml_mapping = _normalize_yarrrml_key_names(yarrrml_mapping)

    yarrrml_mapping = _add_default_prefixes(yarrrml_mapping)
    if 'external' in yarrrml_mapping:
        yarrrml_mapping = _replace_yarrrml_external_references(yarrrml_mapping, yarrrml_mapping['external'])
        yarrrml_mapping.pop('external')
    yarrrml_mapping = _expand_prefixes_in_yarrrml_templates(yarrrml_mapping, yarrrml_mapping['prefixes'])
    prefixes = yarrrml_mapping.pop('prefixes')

    yarrrml_mapping = _normalize_yarrrml_mapping(yarrrml_mapping, prefixes)
    rml_mapping = _translate_yarrrml_to_rml(yarrrml_mapping)

    return rml_mapping
