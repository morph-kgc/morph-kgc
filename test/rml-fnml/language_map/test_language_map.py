__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

"""
Language maps whose value is computed by a function, i.e.
rml:languageMap [ rml:functionExecution ... ].

The statements are compared as text: rdflib parses no triple terms, and it
compares language tags without regard to case.
"""

import os

import pytest

import morph_kgc

from morph_kgc.config.loaders import load_config
from morph_kgc.materializer.pipeline import materialize_pipeline


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def _assert_materializes_to(mapping_file, expected_file):
    mapping_path = os.path.join(TEST_DIR, mapping_file)
    config = (
        f'[CONFIGURATION]\noutput_format=N-TRIPLES\nnumber_of_processes=1\n'
        f'[DataSource]\nmappings={mapping_path}'
    )
    with open(os.path.join(TEST_DIR, expected_file), encoding='utf-8') as f:
        expected = {line.strip()[:-1].strip() for line in f if line.strip()}

    assert {triple.strip() for triple in morph_kgc.materialize_set(config)} == expected


def test_function_valued_language_map():
    _assert_materializes_to('mapping.ttl', 'output.nt')


def test_function_valued_language_map_in_triple_term():
    _assert_materializes_to('mapping_triple_term.ttl', 'output_triple_term.nt')


# A rule whose language or datatype is computed (or read from the data) may
# generate the same literal as a rule with a constant one. Both must then fall
# in the same mapping partition, or the triple is written once per partition.
FUNCTIONS = (
    '<#LowerCaseLanguage> rml:function grel:toLowerCase ;\n'
    '    rml:input [ rml:parameter grel:valueParameter ;\n'
    '                rml:inputValueMap [ rml:reference "Lang" ] ] .\n'
    '<#Datatype> rml:function grel:toLowerCase ;\n'
    '    rml:input [ rml:parameter grel:valueParameter ;\n'
    '                rml:inputValueMap [ rml:constant "http://example.com/DATATYPE" ] ] .\n'
)


@pytest.mark.parametrize('partitioning', ['PARTIAL-AGGREGATIONS', 'MAXIMAL'])
@pytest.mark.parametrize('constant, computed, expected', [
    (
        'rml:language "en"',
        'rml:languageMap [ rml:functionExecution <#LowerCaseLanguage> ]',
        [('1', '"Alice"@en'), ('2', '"Bob"@en'), ('2', '"Bob"@es')],
    ),
    (
        'rml:language "EN"',
        'rml:languageMap [ rml:reference "Lang" ]',
        [('1', '"Alice"@EN'), ('2', '"Bob"@EN'), ('2', '"Bob"@ES')],
    ),
    (
        'rml:language "EN"',
        'rml:languageMap [ rml:template "{Lang}" ]',
        [('1', '"Alice"@EN'), ('2', '"Bob"@EN'), ('2', '"Bob"@ES')],
    ),
    (
        'rml:datatype <http://example.com/datatype>',
        'rml:datatypeMap [ rml:functionExecution <#Datatype> ]',
        [('1', '"Alice"^^<http://example.com/datatype>'),
         ('2', '"Bob"^^<http://example.com/datatype>')],
    ),
], ids=['function-language', 'reference-language', 'template-language', 'function-datatype'])
def test_computed_and_constant_literal_types_not_duplicated(
        tmp_path, partitioning, constant, computed, expected):
    mapping = tmp_path / 'mapping.ttl'
    mapping.write_text(
        '@prefix rml: <http://w3id.org/rml/> .\n'
        '@prefix ex: <http://example.com/> .\n'
        '@prefix grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#> .\n'
        '@base <http://example.com/base/> .\n\n'
        '<TriplesMap1>\n'
        '    rml:logicalSource [ rml:source "test/rml-fnml/language_map/data.csv" ;\n'
        '                        rml:referenceFormulation rml:CSV ] ;\n'
        '    rml:subjectMap [ rml:template "http://example.com/{Id}" ] ;\n'
        f'    rml:predicateObjectMap [ rml:predicate ex:name ;\n'
        f'        rml:objectMap [ rml:reference "Name" ; {constant} ] ] ;\n'
        f'    rml:predicateObjectMap [ rml:predicate ex:name ;\n'
        f'        rml:objectMap [ rml:reference "Name" ; {computed} ] ] .\n\n'
        + FUNCTIONS,
        encoding='utf-8',
    )
    output = tmp_path / 'output.nt'
    config = load_config(
        f'[CONFIGURATION]\n'
        f'output_format=N-TRIPLES\n'
        f'output_file={output.as_posix()}\n'
        f'mapping_partitioning={partitioning}\n'
        f'number_of_processes=1\n'
        f'[DataSource]\n'
        f'mappings={mapping.as_posix()}'
    )
    materialize_pipeline(config, output='file')

    lines = [line.strip() for line in output.read_text(encoding='utf-8').splitlines() if line.strip()]
    assert sorted(lines) == sorted(
        f'<http://example.com/{id_}> <http://example.com/name> {literal} .'
        for id_, literal in expected
    )
