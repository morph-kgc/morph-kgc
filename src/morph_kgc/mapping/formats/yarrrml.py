__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import rdflib

from ruamel.yaml import YAML
from copy import deepcopy
import json
import re

from ...constants import *


# dictionary mapping reference formulations in YARRRML to RML
REFERENCE_FORMULATION_DICT = {
    'csv': RML_CSV,
    'jsonpath': RML_JSONPATH,
    'xpath': RML_XPATH,
    'cypher': RML_CYPHER,
    'sql2008': RML_SQL2008,
    'geoparquet': RML_GEOPARQUET,
    'shapefile': RML_SHP
}

# an escaped `$(` is a literal `$(`, anything else within `$(` and `)` is a reference
YARRRML_REFERENCE = re.compile(r'\\\$\(|\$\(([^)]*)\)')
# a YARRRML template made up of exactly one reference and nothing else
YARRRML_SINGLE_REFERENCE = re.compile(r'^\$\(([^)]*)\)$')
# an external reference, optionally escaped as `$(\_name)`
YARRRML_EXTERNAL_REFERENCE = re.compile(r'\$\((\\?)_([^)]*)\)')

# IRI schemes that a YARRRML value may spell out in full. Values that expand from a prefix are
# recognized as IRIs by `_PrefixedIRI` instead, so this only needs the schemes commonly written
# out by hand: anything else stays a literal, as YARRRML requires `~iri` to force an IRI.
WRITTEN_OUT_IRI_SCHEMES = ('http://', 'https://', 'ftp://', 'ftps://')

# idlab-fn function used to only generate a term when a condition holds
IDLAB_TRUE_CONDITION = 'https://w3id.org/imec/idlab/function#trueCondition'
IDLAB_STR = 'https://w3id.org/imec/idlab/function#str'
IDLAB_STR_BOOLEAN = 'https://w3id.org/imec/idlab/function#strBoolean'


class _PrefixedIRI(str):
    """A value that came from expanding a YARRRML prefix, and is therefore known to be an IRI.

    YARRRML gives no other way of telling an IRI apart from a literal that happens to contain a
    colon, so the information is kept on the string itself when the prefix is expanded.
    """


def _as_list(yarrrml_value):
    # YARRRML allows most values to be given either on their own or as a list of them
    if yarrrml_value is None:
        return []
    return yarrrml_value if type(yarrrml_value) is list else [yarrrml_value]


def _escape_rml_template(constant_string):
    # curly braces delimit references in an RML template but are literal characters in YARRRML,
    # so they have to be escaped when crossing over
    return constant_string.replace('\\', '\\\\').replace('{', '\\{').replace('}', '\\}')


def _template_to_rml(yarrrml_template):
    rml_template = ''

    constant_ini_pos = 0
    for reference in YARRRML_REFERENCE.finditer(yarrrml_template):
        rml_template += _escape_rml_template(yarrrml_template[constant_ini_pos:reference.start()])
        if reference.group(0) == '\\$(':
            # an escaped `$(` does not start a reference, it is the constant string `$(`
            rml_template += '$('
        else:
            rml_template += f'{{{reference.group(1)}}}'
        constant_ini_pos = reference.end()

    # final constant string
    rml_template += _escape_rml_template(yarrrml_template[constant_ini_pos:])

    return rml_template


def _has_reference(yarrrml_template):
    # escaped `$(` are matched as well, they are constant strings and not references
    return any(reference.group(0) != '\\$(' for reference in YARRRML_REFERENCE.finditer(yarrrml_template))


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
        reference_formulation = str(source['referenceFormulation'])
        if reference_formulation.lower() not in REFERENCE_FORMULATION_DICT:
            raise ValueError(f'Found an invalid reference formulation `{reference_formulation}` in YARRRML mapping. '
                             f'Valid reference formulations are: {", ".join(sorted(REFERENCE_FORMULATION_DICT))}.')
        mapping_graph.add((source_bnode, rdflib.term.URIRef(RML_REFERENCE_FORMULATION),
                           rdflib.term.URIRef(REFERENCE_FORMULATION_DICT[reference_formulation.lower()])))

    return mapping_graph


def _is_iri(yarrrml_template):
    if re.search(r'\s', yarrrml_template):
        # whitespace is not allowed in an IRI, so this is a literal that only happens to start
        # with something that looks like a prefix or a scheme
        return False
    # a value that expanded from a prefix is an IRI, and so is one that spells out its scheme
    return isinstance(yarrrml_template, _PrefixedIRI) or yarrrml_template.startswith(WRITTEN_OUT_IRI_SCHEMES)


def _add_template(mapping_graph, term_map_bnode, yarrrml_template, constant_is_iri=False):
    if not isinstance(yarrrml_template, str):
        # YAML gives numbers and booleans their Python type, but a term map value is always a string
        yarrrml_template = str(yarrrml_template)

    single_reference = YARRRML_SINGLE_REFERENCE.match(yarrrml_template)
    if single_reference:
        # a YARRRML template may be composed of simply one reference
        # in that case the YARRRML template corresponds to an RML reference
        mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_REFERENCE), rdflib.term.Literal(single_reference.group(1))))
    elif _has_reference(yarrrml_template):
        rml_template = _template_to_rml(yarrrml_template)
        mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_TEMPLATE), rdflib.term.Literal(rml_template)))
    elif 'a' == yarrrml_template:
        mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(RDF_TYPE)))
    else:
        # a YARRRML template may have 0 references
        # in that case the YARRRML template corresponds to an RML constant
        constant = yarrrml_template.replace('\\$(', '$(')
        # subjects, predicates and graphs are never literals, objects are unless they are IRIs
        if constant_is_iri or _is_iri(yarrrml_template):
            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(constant)))
        else:
            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.Literal(constant)))

    return mapping_graph


def _normalize_yarrrml_key_names(mappings):
    if type(mappings) is dict:
        for key, value in mappings.copy().items():
            if key in ['mapping', 'm']:
                mappings['mappings'] = mappings.pop(key)
            elif key in ['source']:
                mappings['sources'] = mappings.pop(key)
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
    # the prefixes declared in the mapping take precedence over the default ones
    mappings['prefixes'] = {**default_prefixes, **mappings['prefixes']} if 'prefixes' in mappings else default_prefixes

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
                # the whole value is an external reference, keep the type it was given in YAML
                return external_references_value

        def _replace_external_reference(external_reference):
            escaped, external_references_key = external_reference.group(1), external_reference.group(2)
            if escaped:
                # comply with example 110 in YARRRML spec
                return f'$(_{external_references_key})'
            elif external_references_key in external_references:
                return str(external_references[external_references_key])
            return external_reference.group(0)

        # external references can also be embedded in a template, e.g. `http://$(_host)/$(id)`
        mappings = YARRRML_EXTERNAL_REFERENCE.sub(_replace_external_reference, mappings)

    return mappings


def _expand_prefix(yarrrml_value, prefixes):
    # `type` and not `isinstance`, an already expanded value must not be expanded again
    if type(yarrrml_value) is not str or re.search(r'\s', yarrrml_value.split('(')[0]):
        # a prefixed name never has whitespace in it, so a value that does is a literal that only
        # happens to start with something that looks like a prefix. An inline function is a
        # prefixed name too, its arguments are what may have whitespace inside the parenthesis.
        return yarrrml_value

    for prefix_key, prefix_value in prefixes.items():
        if yarrrml_value.startswith(f'{prefix_key}:'):
            # only the leading prefix is expanded, the rest of the value is left untouched. The
            # prefixed names within the arguments of an inline function are expanded when the
            # inline function is parsed, in `_normalize_function_parameters`.
            return _PrefixedIRI(f'{prefix_value}{yarrrml_value[len(prefix_key) + 1:]}')

    return yarrrml_value


def _expand_prefixes_in_yarrrml_templates(mappings, prefixes):
    if type(mappings) is dict:
        for key, value in mappings.items():
            mappings[key] = _expand_prefixes_in_yarrrml_templates(value, prefixes)
    elif type(mappings) is list:
        for i, value in enumerate(mappings):
            mappings[i] = _expand_prefixes_in_yarrrml_templates(value, prefixes)
    else:
        mappings = _expand_prefix(mappings, prefixes)

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
                # a deep copy, otherwise the expanded mappings share their nested values and
                # normalizing one of them corrupts its siblings
                aux_mapping_value = deepcopy(mapping_value)
                aux_mapping_value[property] = property_value
                mappings['mappings'][f'{mapping_key}_-_-_{property}{i}'] = aux_mapping_value
            mappings['mappings'].pop(mapping_key)

    return mappings


def _normalize_property_in_predicateobjects(mappings, property):
    # expand list of the property (e.g. predicate, objects, graphs) inside a preicateobject with independent mappings for each property
    for mapping_key, mapping_value in mappings['mappings'].copy().items():
        if 'predicateobjects' in mapping_value and property in mapping_value['predicateobjects']:
            if type(mapping_value['predicateobjects'][property]) is list:
                for i, property_value in enumerate(mapping_value['predicateobjects'][property]):
                    # a deep copy, otherwise the expanded mappings share their nested values and
                    # normalizing one of them corrupts its siblings
                    aux_mapping_value = deepcopy(mapping_value)
                    aux_mapping_value['predicateobjects'][property] = property_value
                    mappings['mappings'][f'{mapping_key}_-_-_{property}{i}'] = aux_mapping_value
                mappings['mappings'].pop(mapping_key)

    return mappings


def _yarrrml_condition_parameters(condition):
    # the parameters of a condition can be given as a `[parameter, value]` shortcut or as a
    # `{parameter: ..., value: ...}` dictionary
    for parameter in condition['parameters'] if 'parameters' in condition else []:
        if type(parameter) is dict:
            yield parameter['parameter'], parameter['value']
        else:
            yield parameter[0], parameter[1]


def _apply_value_conditions(term_map, value_conditions):
    """
    Wraps a term map in an idlab-fn:trueCondition function for each condition that is not a join,
    so that the term is only generated when every one of the conditions holds. Conditions are
    nested, and nesting them gives the conjunction of all of them.
    """
    if 'function' in term_map:
        conditioned_value = {yarrrml_key: term_map[yarrrml_key]
                             for yarrrml_key in ('function', 'parameters') if yarrrml_key in term_map}
    else:
        conditioned_value = term_map['value']

    for value_condition in value_conditions:
        conditioned_value = {
            'function': IDLAB_TRUE_CONDITION,
            'parameters': [
                {'parameter': IDLAB_STR, 'value': conditioned_value},
                {'parameter': IDLAB_STR_BOOLEAN, 'value': value_condition}
            ]
        }

    # keep the term type, datatype and language of the original term map, replace its value
    conditioned_term_map = {yarrrml_key: yarrrml_value for yarrrml_key, yarrrml_value in term_map.items()
                            if yarrrml_key not in ('condition', 'value', 'function', 'parameters')}
    if 'type' not in conditioned_term_map:
        conditioned_term_map['type'] = 'literal'
    conditioned_term_map.update(conditioned_value)

    return conditioned_term_map


def _normalize_conditional_mappings(mappings):
    """
    Normalizes the conditions of the term maps of every mapping. A condition with the `equal`
    function is a join condition and is kept as such (as a list, a join can have several of them).
    Any other condition restricts when the term is generated, and is turned into an
    idlab-fn:trueCondition function wrapping the value of the term map.
    """
    for mapping_value in mappings['mappings'].values():
        term_maps = []
        if type(mapping_value.get('subjects')) is dict:
            term_maps.append((mapping_value, 'subjects'))
        if 'predicateobjects' in mapping_value:
            predicateobjects = mapping_value['predicateobjects']
            if 'condition' in predicateobjects and type(predicateobjects.get('objects')) is dict:
                # a condition next to `objects` applies to the object term map
                objects = predicateobjects['objects']
                objects['condition'] = _as_list(objects.get('condition')) + _as_list(predicateobjects['condition'])
                predicateobjects.pop('condition')
            for position in ('predicates', 'objects', 'graphs'):
                if type(predicateobjects.get(position)) is dict:
                    term_maps.append((predicateobjects, position))

        for term_map_holder, term_map_key in term_maps:
            term_map = term_map_holder[term_map_key]
            if 'condition' not in term_map:
                continue

            join_conditions, value_conditions = [], []
            for condition in _as_list(term_map['condition']):
                # `equal` is the only condition that is a join, the rest restrict the term
                if type(condition) is dict and condition.get('function') == 'equal':
                    join_conditions.append(condition)
                else:
                    value_conditions.append(condition)

            if value_conditions:
                term_map = _apply_value_conditions(term_map, value_conditions)
            else:
                term_map = {yarrrml_key: yarrrml_value for yarrrml_key, yarrrml_value in term_map.items()
                            if yarrrml_key != 'condition'}
            if join_conditions:
                term_map['condition'] = join_conditions
            term_map_holder[term_map_key] = term_map

    return mappings


def _split_inline_function_inputs(inline_inputs):
    # split the inputs of an inline function on the commas that are not inside a quoted value
    inputs, input, quote = [], '', None
    for character in inline_inputs:
        if quote:
            quote = None if character == quote else quote
            input += character
        elif character in ('"', "'"):
            quote = character
            input += character
        elif character == ',':
            inputs.append(input)
            input = ''
        else:
            input += character

    if input.strip():
        inputs.append(input)

    return inputs


def _split_inline_function_input(input, inline_function):
    # split an input of an inline function on the first `=` that is not inside a quoted value
    quote = None
    for i, character in enumerate(input):
        if quote:
            quote = None if character == quote else quote
        elif character in ('"', "'"):
            quote = character
        elif character == '=':
            return input[:i].strip(), input[i + 1:].strip()

    raise ValueError(f'Found the input `{input.strip()}` without a value in the inline function '
                     f'`{inline_function}` in YARRRML mapping.')


def _normalize_function_parameters(term_map, prefixes):
    if type(term_map) is dict and 'parameters' in term_map:
        if type(term_map['parameters']) is list:
            for i, parameter in enumerate(term_map['parameters']):
                if type(parameter) is list:
                    term_map['parameters'][i] = {'parameter': parameter[0], 'value': parameter[1]}

                if type(term_map['parameters'][i]['value']) is dict and 'function' in term_map['parameters'][i]['value']:
                    # the value of a parameter can be another function (a composite function)
                    term_map['parameters'][i]['value'] = _normalize_function_parameters(
                        term_map['parameters'][i]['value'], prefixes)
    elif type(term_map) is dict and isinstance(term_map.get('function'), str) and term_map['function'].endswith(')'):
        # inline function examples 99 & 101 YARRRML spec
        inline_function = term_map['function']
        function_id_end_pos = inline_function.find('(')
        function_id = inline_function[:function_id_end_pos].strip()
        # get the parameters by removing the function id and the enclosing parenthesis
        inline_inputs = inline_function[function_id_end_pos + 1:-1]

        inline_parameters = []
        for input in _split_inline_function_inputs(inline_inputs):
            input_parameter, input_value = _split_inline_function_input(input, inline_function)
            # the prefixed names of an inline function are within its arguments, so they are only
            # expanded now, once the arguments have been parsed. A quoted value is a literal and
            # never starts with a prefix, so it is left alone.
            input_parameter, input_value = _expand_prefix(input_parameter, prefixes), _expand_prefix(input_value, prefixes)
            if len(input_value) > 1 and (input_value.startswith('"') and input_value.endswith('"') or
                                         input_value.startswith("'") and input_value.endswith("'")):
                # remove the quotes from the value
                input_value = input_value[1:-1]
            if not input_parameter.startswith('http') and ':' not in input_parameter:
                # the prefix of the parameter is the same as the prefix of the function
                for included_prefix in prefixes.values():
                    if function_id.startswith(included_prefix):
                        input_parameter = included_prefix + input_parameter
                        break
            inline_parameters.append({'parameter': input_parameter, 'value': input_value})

        # final normalized term map
        term_map = {'function': function_id}
        if inline_parameters:
            term_map['parameters'] = inline_parameters

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
        if 'sources' not in mapping_value:
            raise ValueError(f'Found the mapping `{mapping_key}` without a source in YARRRML mapping.')
        if type(mapping_value['sources']) is list:
            for i, source in enumerate(mapping_value['sources']):
                mapping_value['sources'][i] = _expand_source_shortcut(source)

    # replace sources references with actual sources inside the mapping
    if 'sources' in mappings:
        for mapping_key, mapping_value in mappings['mappings'].items():
            if isinstance(mapping_value['sources'], str):
                mappings['mappings'][mapping_key]['sources'] = mappings['sources'][mapping_value['sources']]
            elif type(mapping_value['sources']) is list:
                for i, source in enumerate(mapping_value['sources']):
                    if isinstance(source, str):
                        mappings['mappings'][mapping_key]['sources'][i] = mappings['sources'][source]
                    elif type(source) is dict and 'access' in source and source['access'] in mappings['sources']:
                        # Handle expanded shortcuts like {'access': 'source_name'}
                        # Replace with the actual source definition
                        mappings['mappings'][mapping_key]['sources'][i] = mappings['sources'][source['access']]
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
                    if lang_datatype.endswith('~lang'):
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
                if isinstance(mapping_value['subjects'], str):
                    if mapping_value['subjects'].endswith(('~iri', '~blanknode')):
                        value, termtype = mapping_value['subjects'].split('~')
                        mapping_value['subjects'] = {'value': value, 'type': termtype}
                    else:
                        mapping_value['subjects'] = {'value': mapping_value['subjects']}
            if 'objects' in mapping_value['predicateobjects']:
                if isinstance(mapping_value['predicateobjects']['objects'], str):
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
                term_map = mapping_value['predicateobjects'].get(position)
                if type(term_map) is dict and 'function' in term_map:
                    term_map.update(_normalize_function_parameters(term_map, prefixes))

    #############################################################################
    ############################ INVERSE PREDICATES #############################
    #############################################################################

    for mapping_key, mapping_value in mappings['mappings'].copy().items():
        if 'predicateobjects' in mapping_value:
            if 'inversepredicates' in mapping_value['predicateobjects']:
                # if inversepredicates is not a list make it a list of one element to simplify processing
                inverse_predicates = _as_list(mapping_value['predicateobjects']['inversepredicates'])
                mapping_value['predicateobjects'].pop('inversepredicates')

                for i, inverse_predicate in enumerate(inverse_predicates):
                    inverse_mapping_value = deepcopy(mapping_value)
                    inverse_mapping_value['subjects'] = deepcopy(mapping_value['predicateobjects']['objects'])
                    inverse_mapping_value['predicateobjects'] = {
                        'predicates': inverse_predicate,
                        'objects': deepcopy(mapping_value['subjects'])
                    }
                    # the id must not contain `_-_-_`, an inverse triples map has its own subject map
                    # and is therefore never the target of a referencing object map
                    mappings['mappings'][f'{mapping_key}_inverse{i}'] = inverse_mapping_value

    return mappings


def _translate_yarrrml_function_to_rml(mapping_graph, function, term_map):
    # the datatype, language and term type of the term map are added by the caller, which is also
    # the one that knows the position (subject, predicate, object or graph) the term map is in

    execution_bnode = rdflib.term.BNode()
    mapping_graph.add((term_map, rdflib.term.URIRef(RML_EXECUTION), execution_bnode))

    function_bnode = rdflib.term.BNode()
    mapping_graph.add((execution_bnode, rdflib.term.URIRef(RML_FUNCTION_MAP), function_bnode))
    mapping_graph.add((function_bnode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(function['function'])))

    if 'parameters' in function:
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


def _reference_in_condition(condition_value):
    # the operands of a join condition are references, e.g. `$(id)`
    single_reference = YARRRML_SINGLE_REFERENCE.match(str(condition_value))
    return single_reference.group(1) if single_reference else str(condition_value)


def _add_join_conditions(mapping_graph, term_map_bnode, conditions):
    for condition in conditions:
        children, parents = [], []
        for condition_parameter, condition_value in _yarrrml_condition_parameters(condition):
            if condition_parameter == 'str1':
                children.append(condition_value)
            elif condition_parameter == 'str2':
                parents.append(condition_value)

        # a condition can compare several pairs of references, each of them is a join condition of
        # its own, otherwise which child goes with which parent would be lost
        for child, parent in zip(children, parents):
            join_condition_bnode = rdflib.term.BNode()
            mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_JOIN_CONDITION), join_condition_bnode))
            mapping_graph.add((join_condition_bnode, rdflib.term.URIRef(RML_CHILD),
                               rdflib.term.Literal(_reference_in_condition(child))))
            mapping_graph.add((join_condition_bnode, rdflib.term.URIRef(RML_PARENT),
                               rdflib.term.Literal(_reference_in_condition(parent))))

    return mapping_graph


def _parent_triples_maps(yarrrml_mapping, tm_id_to_norm_tm_ids, parent_mapping_id):
    """
    Returns the triples maps a referencing object map has to join with. A mapping is normalized
    into several triples maps, but only the logical source and the subject map of a triples map
    take part in a join. Triples maps that only differ in their predicate-object maps are
    therefore the same join target and just one of them is kept.
    """
    if parent_mapping_id not in tm_id_to_norm_tm_ids:
        raise ValueError(f'Found a reference to the mapping `{parent_mapping_id}`, '
                         f'which does not exist in YARRRML mapping.')

    parent_triples_maps, join_targets = [], set()
    for norm_tm_id in tm_id_to_norm_tm_ids[parent_mapping_id]:
        norm_tm = yarrrml_mapping['mappings'][norm_tm_id]
        join_target = json.dumps([norm_tm.get('sources'), norm_tm.get('subjects')], sort_keys=True, default=str)
        if join_target not in join_targets:
            join_targets.add(join_target)
            parent_triples_maps.append(norm_tm_id)

    return parent_triples_maps


def _translate_yarrrml_to_rml(yarrrml_mapping):
    tm_id_to_norm_tm_ids = {}
    for mapping_id, mapping_value in yarrrml_mapping['mappings'].items():
        if '_-_-_' in mapping_id:
            orig_mapping_id = mapping_id.split('_-_-_')[0]
        else:
            orig_mapping_id = mapping_id

        # a list and not a set, the triples map a referencing object map joins with must not
        # depend on the iteration order of a set
        if orig_mapping_id in tm_id_to_norm_tm_ids:
            tm_id_to_norm_tm_ids[orig_mapping_id].append(mapping_id)
        else:
            tm_id_to_norm_tm_ids[orig_mapping_id] = [mapping_id]

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
            if isinstance(mapping_value['subjects'], str):
                mapping_graph = _add_template(mapping_graph, subject_bnode, mapping_value['subjects'], constant_is_iri=True)
            elif type(mapping_value['subjects']) is dict:
                if 'function' in mapping_value['subjects']:
                    mapping_graph = _translate_yarrrml_function_to_rml(mapping_graph, mapping_value['subjects'], subject_bnode)
                else:
                    mapping_graph = _add_template(mapping_graph, subject_bnode, mapping_value['subjects']['value'], constant_is_iri=True)

                if 'condition' in mapping_value['subjects']:
                    mapping_graph = _add_join_conditions(mapping_graph, subject_bnode, mapping_value['subjects']['condition'])
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
            # graphs are moved to the predicate-object maps when there are any, so this is only
            # reached when the mapping has none. rml:graphMap belongs to the subject map, a
            # triples map cannot have one.
            graph_bnode = rdflib.BNode()
            mapping_graph.add((subject_bnode, rdflib.term.URIRef(RML_GRAPH_MAP), graph_bnode))
            mapping_graph = _add_template(mapping_graph, graph_bnode, mapping_value['graphs'], constant_is_iri=True)

        ####################### PREDICATE OBJECTS ############
        if 'predicateobjects' in mapping_value:
            predicateobject_bnode = rdflib.BNode()
            mapping_graph.add((triples_map_iri, rdflib.term.URIRef(RML_PREDICATE_OBJECT_MAP), predicateobject_bnode))

            for position, property in zip(['predicates', 'objects', 'graphs'], [RML_PREDICATE_MAP, RML_OBJECT_MAP, RML_GRAPH_MAP]):
                if position in mapping_value['predicateobjects']:
                    term_map = mapping_value['predicateobjects'][position]
                    # only objects can be literals, predicates and graphs are always IRIs
                    constant_is_iri = position != 'objects'
                    # a term map is usually translated to one RML term map, but a referencing
                    # object map needs one for each triples map it joins with
                    term_map_bnodes = []

                    if isinstance(term_map, str):
                        # template
                        term_map_bnode = rdflib.BNode()
                        mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                        mapping_graph = _add_template(mapping_graph, term_map_bnode, term_map, constant_is_iri=constant_is_iri)
                        term_map_bnodes.append(term_map_bnode)
                    elif type(term_map) is dict:
                        if 'function' in term_map:
                            term_map_bnode = rdflib.BNode()
                            mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                            mapping_graph = _translate_yarrrml_function_to_rml(mapping_graph, term_map, term_map_bnode)
                            term_map_bnodes.append(term_map_bnode)
                        elif 'mappings' in term_map:
                            # referencing object map
                            for parent_triples_map in _parent_triples_maps(yarrrml_mapping, tm_id_to_norm_tm_ids, term_map['mappings']):
                                term_map_bnode = rdflib.BNode()
                                mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                                mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_PARENT_TRIPLES_MAP),
                                                   rdflib.term.URIRef(parent_triples_map)))
                                term_map_bnodes.append(term_map_bnode)
                        else:
                            # object dict
                            term_map_bnode = rdflib.BNode()
                            mapping_graph.add((predicateobject_bnode, rdflib.term.URIRef(property), term_map_bnode))
                            mapping_graph = _add_template(mapping_graph, term_map_bnode, term_map['value'], constant_is_iri=constant_is_iri)
                            term_map_bnodes.append(term_map_bnode)

                        for term_map_bnode in term_map_bnodes:
                            if 'condition' in term_map:
                                mapping_graph = _add_join_conditions(mapping_graph, term_map_bnode, term_map['condition'])
                            if 'language' in term_map:
                                mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_LANGUAGE_SHORTCUT), rdflib.term.Literal(term_map['language'])))
                            elif 'datatype' in term_map:
                                mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_DATATYPE_SHORTCUT), rdflib.term.URIRef(term_map['datatype'])))
                            elif 'type' in term_map:
                                if term_map['type'] == 'iri':
                                    mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_IRI)))
                                elif term_map['type'] == 'literal':
                                    mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_LITERAL)))
                                elif term_map['type'] == 'blanknode':
                                    mapping_graph.add((term_map_bnode, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_BLANK_NODE)))
                                else:
                                    raise ValueError(f"Found an invalid termtype `{term_map['type']}` in YARRRML mapping.")

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
