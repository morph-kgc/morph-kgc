__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import pytest
import rdflib
import rdflib.compare

from morph_kgc.mapping.formats.yarrrml import load_yarrrml
from morph_kgc.constants import (
    RML_CHILD, RML_CONSTANT, RML_DATATYPE_SHORTCUT, RML_GRAPH_MAP, RML_JOIN_CONDITION, RML_LANGUAGE_SHORTCUT,
    RML_LOGICAL_SOURCE, RML_PARENT, RML_PARENT_TRIPLES_MAP, RML_PREDICATE_MAP,
    RML_REFERENCE, RML_REFERENCE_FORMULATION, RML_SUBJECT_MAP, RML_TEMPLATE, RML_CSV
)


def translate(tmp_path, yarrrml_mapping):
    mapping_file = os.path.join(tmp_path, 'mapping.yarrrml')
    with open(mapping_file, 'w') as f:
        f.write(yarrrml_mapping)

    return load_yarrrml(mapping_file)


def objects(mapping_graph, predicate):
    return {mapping_object for _, _, mapping_object in mapping_graph.triples((None, rdflib.term.URIRef(predicate), None))}


def triples_maps(mapping_graph):
    return {str(triples_map) for triples_map, _, _
            in mapping_graph.triples((None, rdflib.term.URIRef(RML_LOGICAL_SOURCE), None))}


####################################################################################
############################ EXPANSION OF LISTS ####################################
####################################################################################

def test_lists_of_subjects_and_objects_are_fully_expanded(tmp_path):
    # a mapping with a list of subjects and a list of objects must yield one triples map for
    # every combination of them, none of them can be lost
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            subjects: [ex:a_$(id), ex:b_$(id)]
            po:
              - p: ex:name
                o: [$(first), $(last)]
    ''')

    assert len(triples_maps(mapping_graph)) == 4
    assert objects(mapping_graph, RML_TEMPLATE) == {rdflib.term.Literal('http://ex.com/a_{id}'),
                                                   rdflib.term.Literal('http://ex.com/b_{id}')}
    assert objects(mapping_graph, RML_REFERENCE) == {rdflib.term.Literal('first'), rdflib.term.Literal('last')}


def test_lists_of_sources_and_predicates_are_fully_expanded(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          m1:
            sources: [['d1.csv~csv'], ['d2.csv~csv']]
            s: ex:$(id)
            po:
              - p: [ex:p1, ex:p2]
                o: $(v)
    ''')

    assert len(triples_maps(mapping_graph)) == 4
    assert objects(mapping_graph, RML_PREDICATE_MAP) != set()


def test_subject_graphs_and_predicateobject_graphs_are_combined(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            subjects: [ex:a_$(id), ex:b_$(id)]
            graphs: ex:gSubj
            po:
              - p: ex:name
                o: $(name)
                graphs: ex:gPO
    ''')

    # every subject must end up in both graphs
    assert len(triples_maps(mapping_graph)) == 4
    graphs = {mapping_object for graph_map in objects(mapping_graph, RML_GRAPH_MAP)
              for mapping_object in mapping_graph.objects(graph_map, rdflib.term.URIRef(RML_CONSTANT))}
    assert graphs == {rdflib.term.URIRef('http://ex.com/gSubj'), rdflib.term.URIRef('http://ex.com/gPO')}


####################################################################################
############################ TEMPLATES AND CONSTANTS ###############################
####################################################################################

@pytest.mark.parametrize('yarrrml_object, rml_predicate, rml_object', [
    ('"$(name) is a person"', RML_TEMPLATE, '{name} is a person'),
    ('"$(name)"', RML_REFERENCE, 'name'),
    ('"prefix $(name)"', RML_TEMPLATE, 'prefix {name}'),
    # curly braces are literal characters in YARRRML, they delimit references in RML
    ('"literal with {braces} and $(ref)"', RML_TEMPLATE, 'literal with \\{braces\\} and {ref}'),
])
def test_templates_and_references(tmp_path, yarrrml_object, rml_predicate, rml_object):
    mapping_graph = translate(tmp_path, f'''
        prefixes: {{ex: "http://ex.com/"}}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po: [[ex:p, {yarrrml_object}]]
    ''')

    assert rdflib.term.Literal(rml_object) in objects(mapping_graph, rml_predicate)


def test_constants_of_a_non_http_prefix_are_iris(tmp_path):
    # a predicate is always an IRI, whatever the scheme of the prefix it was written with
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "urn:example:"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po: [[ex:name, $(v)]]
    ''')

    assert rdflib.term.URIRef('urn:example:name') in objects(mapping_graph, RML_CONSTANT)


def test_a_literal_that_looks_like_a_prefixed_name_is_not_expanded(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po: [[ex:p, "ex:a and ex:b"]]
    ''')

    assert rdflib.term.Literal('ex:a and ex:b') in objects(mapping_graph, RML_CONSTANT)


def test_reference_formulation_is_an_iri(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po: [[ex:p, $(v)]]
    ''')

    assert objects(mapping_graph, RML_REFERENCE_FORMULATION) == {rdflib.term.URIRef(RML_CSV)}


def test_graphs_without_predicateobjects_are_on_the_subject_map(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            graphs: ex:g1
    ''')

    # rml:graphMap belongs to a subject map or a predicate-object map, never to a triples map
    for subject_map in objects(mapping_graph, RML_SUBJECT_MAP):
        assert (subject_map, rdflib.term.URIRef(RML_GRAPH_MAP), None) in mapping_graph
    assert triples_maps(mapping_graph).isdisjoint(
        {str(triples_map) for triples_map, _, _ in mapping_graph.triples((None, rdflib.term.URIRef(RML_GRAPH_MAP), None))})


####################################################################################
############################ PREFIXES AND EXTERNAL REFERENCES ######################
####################################################################################

def test_declared_prefixes_take_precedence_over_default_ones(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes:
          schema: "https://schema.org/"
          dc: "http://purl.org/dc/elements/1.1/"
          ex: "http://ex.com/"
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po:
              - [schema:name, $(name)]
              - [dc:title, $(t)]
    ''')

    constants = objects(mapping_graph, RML_CONSTANT)
    assert rdflib.term.URIRef('https://schema.org/name') in constants
    assert rdflib.term.URIRef('http://purl.org/dc/elements/1.1/title') in constants


def test_external_references_are_replaced_within_a_template(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        external: {host: "myhost.org", ver: "v2"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: "http://$(_host)/$(_ver)/$(id)"
            po: [[ex:whole, $(_host)]]
    ''')

    assert rdflib.term.Literal('http://myhost.org/v2/{id}') in objects(mapping_graph, RML_TEMPLATE)
    assert rdflib.term.Literal('myhost.org') in objects(mapping_graph, RML_CONSTANT)


####################################################################################
############################ JOINS #################################################
####################################################################################

PARENT_WITH_TWO_SOURCES = '''
    prefixes: {ex: "http://ex.com/"}
    mappings:
      person:
        sources:
          - ['p1.csv~csv']
          - ['p2.csv~csv']
        s: ex:person_$(id)
        po: [[ex:name, $(name)]]
      order:
        sources: [['o.csv~csv']]
        s: ex:order_$(oid)
        po:
          - p: ex:buyer
            o:
              mapping: person
              condition:
                function: equal
                parameters:
                  - [str1, $(pid)]
                  - [str2, $(id)]
'''


def test_a_join_reaches_every_source_of_the_parent_mapping(tmp_path):
    mapping_graph = translate(tmp_path, PARENT_WITH_TWO_SOURCES)

    assert objects(mapping_graph, RML_PARENT_TRIPLES_MAP) == {
        rdflib.term.URIRef('person_-_-_sources0_-_-_predicateobjects0'),
        rdflib.term.URIRef('person_-_-_sources1_-_-_predicateobjects0')
    }


def test_the_translation_of_a_join_is_deterministic(tmp_path):
    # the parent triples maps used to be picked out of a set, which made the translation depend on
    # the iteration order of that set
    translations = [translate(tmp_path, PARENT_WITH_TWO_SOURCES) for _ in range(5)]
    assert all(rdflib.compare.isomorphic(translations[0], translation) for translation in translations[1:])


def test_a_parent_split_only_by_predicateobjects_is_joined_once(tmp_path):
    # only the logical source and the subject map of the parent take part in a join
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          person:
            sources: [['p.csv~csv']]
            s: ex:person_$(id)
            po:
              - [ex:name, $(name)]
              - [ex:age, $(age)]
              - [ex:city, $(city)]
          order:
            sources: [['o.csv~csv']]
            s: ex:order_$(oid)
            po:
              - p: ex:buyer
                o:
                  mapping: person
                  condition: {function: equal, parameters: [[str1, $(pid)], [str2, $(id)]]}
    ''')

    assert len(objects(mapping_graph, RML_PARENT_TRIPLES_MAP)) == 1


JOIN_MAPPING = '''
prefixes: {ex: "http://ex.com/"}
mappings:
  person:
    sources: [['p.csv~csv']]
    s: ex:person_$(id)
    po: [[ex:name, $(name)]]
  order:
    sources: [['o.csv~csv']]
    s: ex:order_$(oid)
    po:
      - p: ex:buyer
        o:
          mapping: person
%s
'''


@pytest.mark.parametrize('yarrrml_condition', [
    # several pairs of references within one condition
    '''          condition:
            function: equal
            parameters:
              - [str1, $(pid)]
              - [str2, $(id)]
              - [str1, $(cc)]
              - [str2, $(cc2)]''',
    # one condition per pair of references
    '''          condition:
            - function: equal
              parameters: [[str1, $(pid)], [str2, $(id)]]
            - function: equal
              parameters: [[str1, $(cc)], [str2, $(cc2)]]''',
    # the parameters of a condition written out instead of as a shortcut
    '''          condition:
            function: equal
            parameters:
              - parameter: str1
                value: $(pid)
              - parameter: str2
                value: $(id)
              - parameter: str1
                value: $(cc)
              - parameter: str2
                value: $(cc2)''',
])
def test_every_join_condition_is_kept_with_its_own_child_and_parent(tmp_path, yarrrml_condition):
    mapping_graph = translate(tmp_path, JOIN_MAPPING % yarrrml_condition)

    # a join condition pairs exactly one child with one parent, so the pairing is not lost
    joins = set()
    for _, _, join_condition in mapping_graph.triples((None, rdflib.term.URIRef(RML_JOIN_CONDITION), None)):
        children = list(mapping_graph.objects(join_condition, rdflib.term.URIRef(RML_CHILD)))
        parents = list(mapping_graph.objects(join_condition, rdflib.term.URIRef(RML_PARENT)))
        assert len(children) == 1 and len(parents) == 1
        joins.add((str(children[0]), str(parents[0])))

    assert joins == {('pid', 'id'), ('cc', 'cc2')}


####################################################################################
############################ FUNCTIONS #############################################
####################################################################################

GREL = 'http://users.ugent.be/~bjdmeest/function/grel.ttl#'


def test_language_of_a_function_term_map_is_a_literal(tmp_path):
    mapping_graph = translate(tmp_path, f'''
        prefixes: {{ex: "http://ex.com/", grel: "{GREL}"}}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po:
              - p: ex:p
                o:
                  function: grel:toUpperCase
                  parameters: [[grel:valueParameter, $(name)]]
                  language: en
    ''')

    assert objects(mapping_graph, RML_LANGUAGE_SHORTCUT) == {rdflib.term.Literal('en')}


def test_a_datatype_containing_a_tilde_is_not_a_language(tmp_path):
    # the widely used GREL namespace contains a `~`
    mapping_graph = translate(tmp_path, f'''
        prefixes: {{ex: "http://ex.com/", gd: "{GREL}"}}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po:
              - [ex:p, [[$(v), gd:myType]]]
    ''')

    assert objects(mapping_graph, RML_DATATYPE_SHORTCUT) == {rdflib.term.URIRef(f'{GREL}myType')}
    assert objects(mapping_graph, RML_LANGUAGE_SHORTCUT) == set()


COMPOSITE_FUNCTION_MAPPING = '''
prefixes: {ex: "http://ex.com/", grel: "%s"}
mappings:
  m1:
    sources: [['d.csv~csv']]
    s: ex:$(id)
    po:
      - p: ex:p
        o:
          function: grel:toUpperCase
          parameters:
            - parameter: grel:valueParameter
              value:
%s
'''


@pytest.mark.parametrize('yarrrml_parameters', [
    # the parameters of the inner function as a shortcut
    '''                function: grel:toLowerCase
                parameters: [[grel:valueParameter, $(name)]]''',
    # the parameters of the inner function written out
    '''                function: grel:toLowerCase
                parameters:
                  - parameter: grel:valueParameter
                    value: $(name)''',
])
def test_composite_functions(tmp_path, yarrrml_parameters):
    mapping_graph = translate(tmp_path, COMPOSITE_FUNCTION_MAPPING % (GREL, yarrrml_parameters))

    constants = objects(mapping_graph, RML_CONSTANT)
    assert rdflib.term.URIRef(f'{GREL}toUpperCase') in constants
    assert rdflib.term.URIRef(f'{GREL}toLowerCase') in constants
    assert rdflib.term.Literal('name') in objects(mapping_graph, RML_REFERENCE)


@pytest.mark.parametrize('yarrrml_function, rml_constants', [
    # a value with a comma in it does not split the arguments of the inline function
    ("grel:string_replace(valueParameter = $(v), p_string_find = ',', p_string_replace = ';')",
     {',', ';'}),
    # the whitespace within a quoted value is part of the value
    ("grel:string_replace(valueParameter = $(v), p_string_replace = 'hello world')",
     {'hello world'}),
    ("grel:string_split(valueParameter = $(v), p_string_sep = ' ')", {' '}),
])
def test_inline_function_arguments(tmp_path, yarrrml_function, rml_constants):
    mapping_graph = translate(tmp_path, f'''
        prefixes: {{ex: "http://ex.com/", grel: "{GREL}"}}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po:
              - p: ex:p
                o:
                  function: "{yarrrml_function}"
    ''')

    assert rml_constants.issubset({str(constant) for constant in objects(mapping_graph, RML_CONSTANT)})


def test_inline_function_expands_the_prefixes_of_its_arguments(tmp_path):
    mapping_graph = translate(tmp_path, f'''
        prefixes: {{ex: "http://ex.com/", grel: "{GREL}"}}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po:
              - p: ex:p
                o:
                  type: iri
                  function: grel:toLowerCase(grel:valueParameter = ex:color_$(Color))
    ''')

    assert rdflib.term.URIRef(f'{GREL}valueParameter') in objects(mapping_graph, RML_CONSTANT)
    assert rdflib.term.Literal('http://ex.com/color_{Color}') in objects(mapping_graph, RML_TEMPLATE)


def test_a_condition_within_an_object_becomes_a_true_condition(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/", idlab: "https://w3id.org/imec/idlab/function#"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po:
              - p: ex:p
                o:
                  value: $(v)
                  condition:
                    function: idlab:notEqual
                    parameters: [[idlab:str, $(v)], [idlab:otherStr, ""]]
    ''')

    constants = objects(mapping_graph, RML_CONSTANT)
    assert rdflib.term.URIRef('https://w3id.org/imec/idlab/function#trueCondition') in constants
    assert rdflib.term.URIRef('https://w3id.org/imec/idlab/function#notEqual') in constants


####################################################################################
############################ INVERSE PREDICATES ####################################
####################################################################################

def test_inverse_predicates_are_deterministic(tmp_path):
    yarrrml_mapping = '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: ex:$(id)
            po:
              - p: ex:hasChild
                i: [ex:hasParent, ex:childOf]
                o: ex:c_$(cid)
    '''

    translations = [translate(tmp_path, yarrrml_mapping) for _ in range(5)]
    assert all(rdflib.compare.isomorphic(translations[0], translation) for translation in translations[1:])
    assert triples_maps(translations[0]) == {
        'm1_-_-_sources0_-_-_predicateobjects0',
        'm1_-_-_sources0_-_-_predicateobjects0_inverse0',
        'm1_-_-_sources0_-_-_predicateobjects0_inverse1'
    }


####################################################################################
############################ INVALID MAPPINGS ######################################
####################################################################################

@pytest.mark.parametrize('yarrrml_mapping, message', [
    ('''
        mappings:
          m1:
            s: http://ex.com/$(id)
            po: [[http://ex.com/p, $(v)]]
    ''', 'without a source'),
    ('''
        mappings:
          m1:
            sources:
              - access: d.jsonl
                referenceFormulation: jsonl
            s: http://ex.com/$(id)
            po: [[http://ex.com/p, $(v)]]
    ''', 'invalid reference formulation'),
    ('''
        mappings:
          m1:
            sources: [['d.csv~csv']]
            s: {value: $(id), type: literal}
            po: [[http://ex.com/p, $(v)]]
    ''', 'invalid termtype'),
])
def test_invalid_mappings_are_reported(tmp_path, yarrrml_mapping, message):
    with pytest.raises(ValueError, match=message):
        translate(tmp_path, yarrrml_mapping)


def test_the_singular_source_key_is_accepted(tmp_path):
    mapping_graph = translate(tmp_path, '''
        prefixes: {ex: "http://ex.com/"}
        mappings:
          m1:
            source: [['d.csv~csv']]
            s: ex:$(id)
            po: [[ex:p, $(v)]]
    ''')

    assert len(triples_maps(mapping_graph)) == 1
