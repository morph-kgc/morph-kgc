__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import re
import sys

import morph_kgc
import pytest

from rdflib import Dataset, URIRef
from morph_kgc.config.model import ResourceConfig
from morph_kgc.testing import assert_isomorphic

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from server import PASSWORD, USERNAME, VocabularyServer   # noqa: E402

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
MAPPING = os.path.join(TEST_DIR, 'mapping.ttl')
VOCABULARY = os.path.join(TEST_DIR, 'disease_vocabulary.ttl')
VOCABULARY_IRI_MAPPING = os.path.join(TEST_DIR, 'mapping_vocabulary_iri.ttl')
UPPERCASE_MAPPING = os.path.join(TEST_DIR, 'mapping_uppercase.ttl')
MATCHING_MODES_MAPPING = os.path.join(TEST_DIR, 'mapping_matching_modes.ttl')

SKOS = 'http://www.w3.org/2004/02/skos/core#'
# The predicate the mappings relate a patient to the reconciled disease with.
HAS_DISEASE = 'http://semanticscience.org/resource/SIO_000255'
ATAXIA = 'https://data.boehringer.com/id/00036/10073037'
NARCOLEPSY = 'https://data.boehringer.com/id/00036/10077339'


def config(resource_options, mapping=MAPPING, processes=1):
    return (
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'number_of_processes={processes}\n'
        f'[RESOURCE:disease_vocabulary]\n'
        f'{resource_options}\n'
        f'[DataSource]\n'
        f'mappings={mapping}'
    )


def expected_graph():
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output.nq'))
    return g


def mapping_copy(tmp_path, *replacements, mapping=MAPPING):
    """A copy of *mapping* with each (old, new) replacement applied."""
    with open(mapping, encoding='utf-8') as f:
        text = f.read()
    for old, new in replacements:
        assert old in text
        text = text.replace(old, new)
    path = tmp_path / os.path.basename(mapping)
    path.write_text(text, encoding='utf-8', newline='\n')
    return str(path)


def reconciled(g_morph, predicate=HAS_DISEASE):
    """The patients *predicate* relates to a concept, and those concepts."""
    return {
        (str(patient).rsplit('/', 1)[1], str(concept))
        for patient, concept in g_morph.subject_objects(URIRef(predicate))
    }


# ── SKOS vocabulary ───────────────────────────────────────────────────────────

def test_reconcile_vocabulary_from_file():
    """A vocabulary declared as a local path is indexed and reconciled against."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}'
    ))

    assert_isomorphic(expected_graph(), g_morph)


def test_reconcile_vocabulary_over_http_with_basic_authentication():
    """The vocabulary is fetched once, with HTTP Basic Authentication."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={server.url}/vocabulary\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}\n'
            f'format=turtle'
        ))

        # Initialized once, before any triple was materialized, even though the
        # mapping reconciles in four mapping groups.
        assert [path for path, _ in server.requests] == ['/vocabulary']

    assert_isomorphic(expected_graph(), g_morph)


def test_reconcile_vocabulary_over_http_without_credentials():
    """A protected vocabulary reports what is missing instead of failing blindly."""
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match='Basic Authentication'):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary'
            ))


@pytest.mark.parametrize('file_name', ['disease_vocabulary.nq', 'disease_vocabulary.trig'])
def test_reconcile_vocabulary_in_named_graphs_from_file(file_name):
    """A vocabulary serialized as quads is indexed across all its named graphs."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={os.path.join(TEST_DIR, file_name)}'
    ))

    assert_isomorphic(expected_graph(), g_morph)


@pytest.mark.parametrize('path', ['/vocabulary.nq', '/vocabulary.trig'])
def test_reconcile_vocabulary_in_named_graphs_over_http(path):
    """
    A vocabulary served as quads is indexed across all its named graphs, even
    when served as text/plain: the extension then says it holds quads.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={server.url}{path}\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}'
        ))

    assert_isomorphic(expected_graph(), g_morph)


@pytest.mark.parametrize('path', ['/vocabulary.gz', '/vocabulary.multi-gz'])
def test_reconcile_vocabulary_served_gzipped(path):
    """
    A vocabulary served gzipped is decompressed as it is downloaded, all its
    members when it has several.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={server.url}{path}\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}'
        ))

    assert_isomorphic(expected_graph(), g_morph)


def test_vocabulary_wrongly_said_to_be_gzipped():
    """A vocabulary that is not gzipped as its server says is reported as such."""
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match='Could not decompress the vocabulary'):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary.broken-gz\n'
                f'username={USERNAME}\n'
                f'password={PASSWORD}'
            ))


@pytest.mark.parametrize('rdf_format, extension', [
    ('xml', 'rdf'),
    ('json-ld', 'jsonld'),
    ('nt', 'nt'),
])
def test_reconcile_vocabulary_in_other_serializations(tmp_path, caplog, rdf_format, extension):
    """A vocabulary in any serialization the streaming parser reads is streamed."""
    g = Dataset()
    g.parse(VOCABULARY)
    vocabulary = tmp_path / f'disease_vocabulary.{extension}'
    g.serialize(vocabulary, format=rdf_format)

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}'
    ))

    assert_isomorphic(expected_graph(), g_morph)
    assert 'parsed in memory' not in caplog.text


def test_vocabulary_the_streaming_parser_cannot_read(tmp_path, caplog):
    """A vocabulary the streaming parser cannot read is parsed in memory instead."""
    g = Dataset()
    g.parse(VOCABULARY)
    vocabulary = tmp_path / 'disease_vocabulary.trix'
    g.serialize(vocabulary, format='trix')

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}\n'
        f'format=trix'
    ))

    assert_isomorphic(expected_graph(), g_morph)
    assert 'so it is parsed in memory' in caplog.text


def test_vocabulary_cut_short():
    """A vocabulary whose server closes the connection halfway is not indexed in part."""
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match='closed .* before the end of the body'):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary.cut\n'
                f'username={USERNAME}\n'
                f'password={PASSWORD}'
            ))


def test_vocabulary_url_that_is_no_iri(caplog):
    """A vocabulary URL that is no IRI ('|') is still a base for its relative IRIs."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={server.url}/export|vocabulary\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}'
        ))

    assert_isomorphic(expected_graph(), g_morph)
    assert 'parsed in memory' not in caplog.text


@pytest.mark.parametrize('format_option, prefix', [
    ('text/turtle', b''),
    ('turtle', b'\xef\xbb\xbf'),
])
def test_vocabulary_streamed_whatever_its_format_option_or_byte_order_mark(
    tmp_path, caplog, format_option, prefix,
):
    """
    A vocabulary whose 'format' is a media type, or which starts with a byte
    order mark, is streamed too.
    """
    vocabulary = tmp_path / 'disease_vocabulary.ttl'
    with open(VOCABULARY, 'rb') as f:
        vocabulary.write_bytes(prefix + f.read())

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}\n'
        f'format={format_option}'
    ))

    assert_isomorphic(expected_graph(), g_morph)
    assert 'parsed in memory' not in caplog.text


def test_json_ld_vocabulary_with_a_context(tmp_path, caplog):
    """
    A JSON-LD vocabulary whose top level is an object, with its @context and
    @graph, is reconciled against, but read whole first, which a warning says.
    """
    g = Dataset()
    g.parse(VOCABULARY)
    vocabulary = tmp_path / 'disease_vocabulary.jsonld'
    g.serialize(vocabulary, format='json-ld', context={'skos': SKOS})
    assert vocabulary.read_text(encoding='utf-8').lstrip().startswith('{')

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}'
    ))

    assert_isomorphic(expected_graph(), g_morph)
    assert 'read whole into memory' in caplog.text


def test_vocabulary_with_a_lone_surrogate_escape(tmp_path, caplog):
    """
    A label holding a lone surrogate escape ("\\uD800"), which the streaming
    parser rejects and rdflib reads, is left out; the others are reconciled.
    """
    vocabulary = tmp_path / 'disease_vocabulary.ttl'
    with open(VOCABULARY, encoding='utf-8') as f:
        vocabulary.write_text(
            f.read() + f'\n<https://example.org/broken> <{SKOS}prefLabel> "broken \\uD800" .\n',
            encoding='utf-8',
        )

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}'
    ))

    assert_isomorphic(expected_graph(), g_morph)
    assert 'so it is parsed in memory' in caplog.text


def test_vocabulary_that_is_not_rdf(tmp_path):
    """A vocabulary neither parser reads says how to declare its serialization."""
    vocabulary = tmp_path / 'disease_vocabulary.ttl'
    vocabulary.write_text('this is not RDF', encoding='utf-8')

    with pytest.raises(ValueError, match="could not be parsed as RDF .*'format' option"):
        morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={vocabulary}'
        ))


def test_index_is_kept_on_disk(tmp_path, monkeypatch):
    """
    The index of the vocabulary is a file of the state directory of the run,
    removed when the run ends, which the shared context only refers to.
    """
    from morph_kgc.functions import executor
    from morph_kgc.functions.state import get_context

    state_dir = tmp_path / 'state'
    files = []

    def spying_get_context(function_iri, run_config):
        files.extend((path.name, path.stat().st_size) for path in state_dir.glob('*/*'))
        return get_context(function_iri, run_config)

    monkeypatch.setattr(executor, 'get_context', spying_get_context)

    g_morph = morph_kgc.materialize(
        config(f'resource_type=SKOS_VOCABULARY\nurl={VOCABULARY}', processes=1)
        .replace('[CONFIGURATION]\n', f'[CONFIGURATION]\nstate_dir={state_dir}\n')
    )

    assert_isomorphic(expected_graph(), g_morph)
    contexts = [size for name, size in files if name.endswith('.pickle')]
    indexes = [size for name, size in files if name.endswith('.parquet')]
    assert contexts and indexes
    # The context holds the path of the index, not its entries.
    assert max(contexts) < 2000
    assert os.listdir(state_dir) == []


def test_index_looks_values_up_alike_one_by_one_or_together(tmp_path):
    """
    An index looks a few values up one by one, in the parts of its file that
    may hold them, and many values in a single pass over it, with the same
    results.
    """
    from dataclasses import replace
    from morph_kgc.functions.tables import close_connections
    from morph_kgc.reconciliation.index import CASE_INSENSITIVE_MATCHING, ConceptIndexBuilder

    with ConceptIndexBuilder(str(tmp_path), '512MB', CASE_INSENSITIVE_MATCHING) as builder:
        builder.add('label', 'Ataxia', 'c1')
        builder.add('alt', 'ATAXIA', 'c2')
        builder.add('label', 'Narcolepsy', 'c3')
        builder.add('label', 'Scheme', 'c4')
        builder.exclude('c4')
        index = builder.build()

    values = ['ataxia', None, 'Narcolepsy', 'unknown', 'Scheme', 'Ataxia ']
    try:
        together = index.lookup_many(values, ['label', 'alt'])
        one_by_one = replace(index, row_groups=100).lookup_many(values, ['label', 'alt'])
    finally:
        close_connections(str(tmp_path))

    assert together == one_by_one == [['c1', 'c2'], [], ['c3'], [], [], ['c1', 'c2']]
    assert (len(index), index.concept_count, index.row_groups) == (3, 3, 1)


def test_index_looks_tuples_of_values_up_alike_one_by_one_or_together(tmp_path):
    """
    An index matching entities by two values looks the tuples up by both, as
    they are: tuples that would look alike joined by '|', ',' or a space are
    told apart, one by one and together.
    """
    from dataclasses import replace
    from morph_kgc.functions.tables import close_connections
    from morph_kgc.reconciliation.index import ConceptIndexBuilder

    with ConceptIndexBuilder(str(tmp_path), '512MB', arity=2) as builder:
        for code, system, concept in [
            ('A|1', 'ICD-10', 'c6'), ('A|1', 'ICD 10', 'c7'), ('A', '1|ICD-10', 'c8'),
            ('B, C', 'X', 'c9'), ('B', ' C,X', 'c10'), ('A B', 'C', 'e1'), ('A', 'B C', 'e2'),
        ]:
            builder.add('', (code, system), concept)
        index = builder.build()

    values = [
        ('A|1', 'ICD-10'), ('A', '1|ICD-10'), ('A|1', 'ICD-11'), ('ICD-10', 'A|1'),
        ('B, C', 'X'), ('B', ' C,X'), ('A B', 'C'), ('A', 'B C'), ('A|1', None),
    ]
    try:
        together = index.lookup_many(values)
        one_by_one = replace(index, row_groups=100).lookup_many(values)
    finally:
        close_connections(str(tmp_path))

    assert together == one_by_one == [
        ['c6'], ['c8'], [], [], ['c9'], ['c10'], ['e1'], ['e2'], [],
    ]
    assert (len(index), index.concept_count, index.arity) == (7, 7, 2)


def test_reconcile_vocabulary_with_multiple_processes():
    """Worker processes read the shared context back from the state directory."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        processes=4,
    ))

    assert_isomorphic(expected_graph(), g_morph)


def test_reconcile_vocabulary_referenced_by_its_iri():
    """
    A mapping may name the vocabulary by the IRI that identifies it, while
    where it is downloaded from stays in the configuration file.
    """
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'iri=https://example.org/vocabulary/disease\n'
        f'url={VOCABULARY}',
        mapping=VOCABULARY_IRI_MAPPING,
    ))

    assert_isomorphic(expected_graph(), g_morph)


def with_matching(matching):
    """
    The replacement adding a matching input to the execution of mapping.ttl or
    mapping_uppercase.ttl.
    """
    return (
        'rml:inputValue skos:prefLabel , skos:altLabel\n    ]',
        'rml:inputValue skos:prefLabel , skos:altLabel\n    ] ;\n'
        '    rml:input [\n'
        '        rml:parameter morph-fn:matching ;\n'
        f'        rml:inputValue {matching}\n'
        '    ]',
    )


def reconciled_graph():
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output_reconciled.nq'))
    return g


@pytest.mark.parametrize('matching', ['"CASE-INSENSITIVE"', '"case-insensitive"', '" Case-Insensitive "'])
def test_reconcile_case_insensitive_matching(tmp_path, matching):
    """
    Case-insensitive matching, which the execution asks for in any case,
    reconciles values that differ in case only.
    """
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=mapping_copy(tmp_path, with_matching(matching), mapping=UPPERCASE_MAPPING),
    ))

    assert_isomorphic(reconciled_graph(), g_morph)


def test_reconcile_from_yarrrml():
    """The same reconciliation expressed in YARRRML instead of RML-FNML."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=os.path.join(TEST_DIR, 'mapping.yarrrml'),
    ))

    assert_isomorphic(reconciled_graph(), g_morph)


@pytest.mark.parametrize('replacements', [(), (with_matching('"EXACT"'),), (with_matching('"exact"'),)])
def test_exact_matching_by_default(tmp_path, replacements):
    """Exact matching is the default, so a value in another case matches nothing."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=mapping_copy(tmp_path, *replacements, mapping=UPPERCASE_MAPPING),
    ))

    assert len(g_morph) == 0


@pytest.mark.parametrize('processes', [1, 4])
def test_executions_matching_differently_download_the_vocabulary_once(processes):
    """
    Two executions reconciling against the same vocabulary, one exactly and the
    other case-insensitively, each match as they ask, in every worker process,
    while the vocabulary is downloaded once.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={server.url}/vocabulary\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}',
            mapping=MATCHING_MODES_MAPPING,
            processes=processes,
        ))

        assert [path for path, _ in server.requests] == ['/vocabulary']

    assert reconciled(g_morph, 'https://example.org/kg/diseaseExact') == {('pid_00002', NARCOLEPSY)}
    assert reconciled(g_morph, 'https://example.org/kg/diseaseCaseInsensitive') == {
        ('pid_00001', ATAXIA), ('pid_00002', NARCOLEPSY),
    }


@pytest.mark.parametrize('attributes, expected', [
    # Without the option, every literal of the vocabulary, its notations too.
    ('', {('pid_00001', ATAXIA), ('pid_00002', NARCOLEPSY), ('pid_00003', NARCOLEPSY)}),
    (f'attributes={SKOS}prefLabel,{SKOS}altLabel', {('pid_00001', ATAXIA), ('pid_00002', NARCOLEPSY)}),
])
def test_executions_matching_differently_name_different_attributes(tmp_path, attributes, expected):
    """
    An execution matching exactly against the attribute it names and one
    matching case-insensitively against those of the resource each get theirs.
    """
    patients = tmp_path / 'patients.csv'
    patients.write_text(
        'pid,disease_label\n'
        'pid_00001,Early-onset spastic ataxia-myoclonic epilepsy-neuropathy syndrome\n'
        'pid_00002,adca-dn\n'
        'pid_00003,orpha:314404\n',
        encoding='utf-8',
    )
    mapping = mapping_copy(
        tmp_path,
        ('test/rml-fnml/reconciliation/patients.csv', patients.as_posix()),
        ('rml:inputValueMap [ rml:reference "disease_label" ]\n    ] .',
         'rml:inputValueMap [ rml:reference "disease_label" ]\n    ] ;\n'
         '    rml:input [\n'
         '        rml:parameter morph-fn:matching ;\n'
         '        rml:inputValue "CASE-INSENSITIVE"\n'
         '    ] .'),
        mapping=os.path.join(TEST_DIR, 'mapping_two_executions.ttl'),
    )

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}\n'
        f'{attributes}',
        mapping=mapping,
    ))

    assert reconciled(g_morph, 'https://example.org/kg/diseaseByAltLabel') == {('pid_00001', ATAXIA)}
    assert reconciled(g_morph, 'https://example.org/kg/disease') == expected


def test_execution_matching_case_insensitively_attributes_from_the_data(tmp_path):
    """
    An execution reading its attributes from the data matches
    case-insensitively against any literal of the vocabulary, next to one
    matching exactly against the attribute it names.
    """
    patients = tmp_path / 'patients.csv'
    patients.write_text(
        'pid,disease_label,attribute\n'
        f'pid_00001,EARLY-ONSET SPASTIC ATAXIA-MYOCLONIC EPILEPSY-NEUROPATHY SYNDROME,{SKOS}altLabel\n'
        f'pid_00002,orpha:314404,{SKOS}notation\n'
        f'pid_00003,ADCA-DN,{SKOS}altLabel\n'
        f'pid_00004,adca-dn,{SKOS}prefLabel\n',
        encoding='utf-8',
    )
    mapping = mapping_copy(
        tmp_path,
        ('test/rml-fnml/reconciliation/patients.csv', patients.as_posix()),
        ('rml:inputValueMap [ rml:reference "disease_label" ]\n    ] .',
         'rml:inputValueMap [ rml:reference "disease_label" ]\n    ] ;\n'
         '    rml:input [\n'
         '        rml:parameter grel:attributeIRI ;\n'
         '        rml:inputValueMap [ rml:reference "attribute" ]\n'
         '    ] ;\n'
         '    rml:input [\n'
         '        rml:parameter morph-fn:matching ;\n'
         '        rml:inputValue "CASE-INSENSITIVE"\n'
         '    ] .'),
        mapping=os.path.join(TEST_DIR, 'mapping_two_executions.ttl'),
    )

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=mapping,
    ))

    assert reconciled(g_morph, 'https://example.org/kg/diseaseByAltLabel') == {('pid_00003', NARCOLEPSY)}
    assert reconciled(g_morph, 'https://example.org/kg/disease') == {
        ('pid_00001', ATAXIA), ('pid_00002', NARCOLEPSY), ('pid_00003', NARCOLEPSY),
    }


@pytest.mark.parametrize('matched_attributes, expected', [
    (
        [('EXACT', (f'{SKOS}prefLabel',)), ('CASE-INSENSITIVE', (f'{SKOS}altLabel',))],
        {'EXACT': {f'{SKOS}prefLabel'}, 'CASE-INSENSITIVE': {f'{SKOS}altLabel'}},
    ),
    (
        # An execution reading its attributes from the data matches against
        # every literal, in its own matching only.
        [('EXACT', (f'{SKOS}prefLabel',)), ('CASE-INSENSITIVE', None)],
        {'EXACT': {f'{SKOS}prefLabel'},
         'CASE-INSENSITIVE': {f'{SKOS}prefLabel', f'{SKOS}altLabel', f'{SKOS}notation'}},
    ),
    (
        [('CASE-INSENSITIVE', (f'{SKOS}altLabel',))],
        {'CASE-INSENSITIVE': {f'{SKOS}altLabel'}},
    ),
])
def test_only_the_matchings_executions_use_are_indexed(tmp_path, matched_attributes, expected):
    """
    A vocabulary gets an index for each way the executions match values against
    it, holding the attributes the executions matching that way match against.
    """
    from morph_kgc.reconciliation import skos

    resource = ResourceConfig(
        name='disease_vocabulary', resource_type='SKOS_VOCABULARY', url=VOCABULARY,
    )
    indexes = skos.build_index(
        resource, matched_attributes, state_dir=str(tmp_path), memory_limit='512MB',
    )

    assert {matching: set(index.indexed_attributes) for matching, index in indexes.items()} == expected


def test_matching_in_legacy_fnml():
    """The matching is a constant value map in the legacy FNML vocabulary."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=os.path.join(TEST_DIR, 'mapping_legacy_fnml.ttl'),
    ))

    assert_isomorphic(reconciled_graph(), g_morph)


@pytest.mark.parametrize('parameter', [
    '                    - parameter: morph-fn:matching\n'
    '                      value: CASE-INSENSITIVE\n',
    '                    - [morph-fn:matching, case-insensitive]\n',
    '                    - [morph-fn:matching, lowercase]\n',
])
def test_matching_in_yarrrml(tmp_path, parameter):
    """The matching is a plain value in YARRRML, which no prefix expands."""
    mapping = mapping_copy(
        tmp_path,
        ('patients.csv', 'patients_uppercase.csv'),
        ('                      value: skos:altLabel\n',
         '                      value: skos:altLabel\n' + parameter),
        mapping=os.path.join(TEST_DIR, 'mapping.yarrrml'),
    )
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=mapping,
    ))

    assert_isomorphic(reconciled_graph(), g_morph)


def test_matching_in_a_yarrrml_inline_function(tmp_path):
    """The matching is an input of a YARRRML inline function as well."""
    mapping = tmp_path / 'mapping.yarrrml'
    mapping.write_text(
        'prefixes:\n'
        '    grel: http://users.ugent.be/~bjdmeest/function/grel.ttl#\n'
        '    morph-fr: "urn:morph:function:reconciliation:"\n'
        '    morph-fn: "urn:morph:function:"\n'
        '    sio: http://semanticscience.org/resource/\n'
        'mappings:\n'
        '    patients:\n'
        '        sources:\n'
        '            - access: test/rml-fnml/reconciliation/patients_uppercase.csv\n'
        '              referenceFormulation: csv\n'
        '        subjects: https://example.org/kg/patient/$(pid)\n'
        '        predicateobjects:\n'
        '            - p: sio:SIO_000255\n'
        '              o:\n'
        '                function: morph-fr:reconcileVocabularyConcept(morph-fn:resource = disease_vocabulary, '
        'grel:valueParam = $(disease_label), morph-fn:matching = CASE-INSENSITIVE)\n'
        '                type: iri\n',
        encoding='utf-8',
    )
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=str(mapping),
    ))

    assert_isomorphic(reconciled_graph(), g_morph)


@pytest.mark.parametrize('alt_labels_matching, expected', [
    ('CASE-INSENSITIVE', {('pid_00002', NARCOLEPSY, 'byUrl'), ('pid_00001', ATAXIA, 'byName')}),
    # Matched exactly, the upper-case labels match no alternative label.
    (None, {('pid_00002', NARCOLEPSY, 'byUrl')}),
])
def test_vocabulary_url_declared_by_two_resources(tmp_path, alt_labels_matching, expected):
    """
    A mapping naming a vocabulary by its URL reconciles against the resource the
    configuration file resolves it to, the first declaring the URL, in the way
    it asks for, even when another resource declares the same URL and is
    reconciled against in another way.
    """
    def execution(name, resource, matching):
        matching_input = (
            '    rml:input [\n'
            '        rml:parameter morph-fn:matching ;\n'
            f'        rml:inputValue "{matching}"\n'
            '    ] ;\n'
        ) if matching else ''
        return (
            f'<#{name}>\n'
            '    rml:function morph-fr:reconcileVocabularyConcept ;\n'
            f'{matching_input}'
            '    rml:input [\n'
            '        rml:parameter morph-fn:resource ;\n'
            f'        rml:inputValue "{resource}"\n'
            '    ] ;\n'
            '    rml:input [\n'
            '        rml:parameter grel:valueParam ;\n'
            '        rml:inputValueMap [ rml:reference "disease_label" ]\n'
            '    ] .\n\n'
        )

    def object_map(predicate, execution_name):
        return (
            '    rml:predicateObjectMap [\n'
            f'        rml:predicate ex:{predicate} ;\n'
            f'        rml:objectMap [ rml:functionExecution <#{execution_name}> ; rml:termType rml:IRI ]\n'
            '    ]'
        )

    # The mapping and the configuration file name the vocabulary by the same
    # URL, written with forward slashes: a backslash of a Windows path would be
    # an escape in the Turtle string of the mapping.
    vocabulary = VOCABULARY.replace(os.sep, '/')
    mapping = tmp_path / 'mapping.ttl'
    mapping.write_text(
        '@prefix rml: <http://w3id.org/rml/> .\n'
        '@prefix grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#> .\n'
        '@prefix morph-fr: <urn:morph:function:reconciliation:> .\n'
        '@prefix morph-fn: <urn:morph:function:> .\n'
        '@prefix ex: <https://example.org/kg/> .\n\n'
        '<#PatientsMapping>\n'
        '    a rml:TriplesMap ;\n'
        '    rml:logicalSource [\n'
        '        rml:source "test/rml-fnml/reconciliation/patients_uppercase.csv" ;\n'
        '        rml:referenceFormulation rml:CSV\n'
        '    ] ;\n'
        '    rml:subjectMap [ rml:template "https://example.org/kg/patient/{pid}" ] ;\n'
        f'{object_map("byUrl", "ByUrl")} ;\n'
        f'{object_map("byName", "ByName")} .\n\n'
        + execution('ByUrl', vocabulary, 'CASE-INSENSITIVE')
        + execution('ByName', 'alt_labels', alt_labels_matching),
        encoding='utf-8',
    )

    g_morph = morph_kgc.materialize(
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'[RESOURCE:pref_labels]\n'
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}\n'
        f'attributes=skos:prefLabel\n'
        f'[RESOURCE:alt_labels]\n'
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}\n'
        f'attributes=skos:altLabel\n'
        f'[DataSource]\n'
        f'mappings={mapping}'
    )

    assert {
        (patient, concept, predicate.rsplit('/', 1)[1])
        for predicate in ('https://example.org/kg/byUrl', 'https://example.org/kg/byName')
        for patient, concept in reconciled(g_morph, predicate)
    } == expected


@pytest.mark.parametrize('binding', [
    'rml:inputValueMap [ rml:reference "disease_label" ]',
    'rml:inputValueMap [ rml:template "{disease_label}" ]',
    'rml:inputValueMap [ rml:functionExecution <#Matching> ]',
    # A constant, but two of them.
    'rml:inputValue "EXACT" , "CASE-INSENSITIVE"',
])
def test_matching_not_given_as_a_single_constant(tmp_path, binding):
    """
    The vocabulary is indexed before any data is read, so how values are matched
    cannot depend on the data. The mistake is reported before it is downloaded.
    """
    old, new = with_matching('"CASE-INSENSITIVE"')
    mapping = mapping_copy(
        tmp_path,
        (old,
         new.replace('rml:inputValue "CASE-INSENSITIVE"', binding) + ' .\n\n'
         '<#Matching>\n'
         '    rml:function grel:toUpperCase ;\n'
         '    rml:input [\n'
         '        rml:parameter grel:valueParameter ;\n'
         '        rml:inputValue "case-insensitive"\n'
         '    ]'),
        mapping=UPPERCASE_MAPPING,
    )
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=r"must bind 'urn:morph:function:matching' at most once, to a constant"):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary\n'
                f'username={USERNAME}\n'
                f'password={PASSWORD}',
                mapping=mapping,
            ))

        assert server.requests == []


@pytest.mark.parametrize('matching', ['FUZZY', 'CASE_INSENSITIVE', 'CASE INSENSITIVE', ''])
def test_invalid_matching(tmp_path, matching):
    """A matching that is not one of those known fails, naming them."""
    error = (
        f"binds 'urn:morph:function:matching' to '{matching}', which is not valid. "
        "Must be one of: CASE-INSENSITIVE, EXACT, LOWERCASE."
    )
    with pytest.raises(ValueError, match=re.escape(error)):
        morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={VOCABULARY}',
            mapping=mapping_copy(tmp_path, with_matching(f'"{matching}"'), mapping=UPPERCASE_MAPPING),
        ))


# The attribute input of mapping.ttl, removed by the tests of executions that
# name no attribute.
WITHOUT_ATTRIBUTE = (
    ' ;\n'
    '    rml:input [\n'
    '        rml:parameter morph-fn:attributeIRI ;\n'
    '        rml:inputValue skos:prefLabel , skos:altLabel\n'
    '    ]',
    '',
)


@pytest.mark.parametrize('attributes, expected', [
    ('skos:prefLabel,skos:altLabel', {('pid_00001', ATAXIA), ('pid_00002', NARCOLEPSY)}),
    ('skos:prefLabel', {('pid_00002', NARCOLEPSY)}),
    (f'{SKOS}prefLabel', {('pid_00002', NARCOLEPSY)}),
])
def test_attributes_option(tmp_path, attributes, expected):
    """
    An execution naming no attribute matches against the 'attributes' option of
    the resource, written as IRIs or as prefixed names.
    """
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}\n'
        f'attributes={attributes}',
        mapping=mapping_copy(tmp_path, WITHOUT_ATTRIBUTE),
    ))

    assert reconciled(g_morph) == expected


@pytest.mark.parametrize('prefix, namespace', [
    ('dcterms', 'http://purl.org/dc/terms/'),
    ('schema', 'https://schema.org/'),
])
def test_attributes_option_with_a_well_known_prefix(tmp_path, prefix, namespace):
    """
    The well-known prefixes of the 'attributes' option stand for their usual
    namespace, whatever the vocabulary declares.
    """
    with open(VOCABULARY, encoding='utf-8') as f:
        text = f.read().replace('skos:altLabel', f'<{namespace}synonym>')
    vocabulary = tmp_path / 'disease_vocabulary.ttl'
    vocabulary.write_text(f'@prefix {prefix}: <https://example.org/other#> .\n' + text, encoding='utf-8')

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}\n'
        f'attributes=skos:prefLabel,{prefix}:synonym',
        mapping=mapping_copy(tmp_path, WITHOUT_ATTRIBUTE),
    ))

    assert_isomorphic(expected_graph(), g_morph)


def test_attributes_option_with_a_prefix_of_the_vocabulary(tmp_path):
    """
    A prefix only the vocabulary declares is not used by the 'attributes'
    option, which says to write the attribute as a full IRI.
    """
    with open(VOCABULARY, encoding='utf-8') as f:
        text = f.read().replace('skos:altLabel', 'dis:synonym')
    vocabulary = tmp_path / 'disease_vocabulary.ttl'
    vocabulary.write_text('@prefix dis: <https://example.org/vocabulary/disease#> .\n' + text, encoding='utf-8')

    error = "'dis:synonym', which is neither an absolute IRI .* Write it as a full IRI"
    with pytest.raises(ValueError, match=error):
        morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={vocabulary}\n'
            f'attributes=skos:prefLabel,dis:synonym',
            mapping=mapping_copy(tmp_path, WITHOUT_ATTRIBUTE),
        ))


@pytest.mark.parametrize('iri', [
    f'{SKOS}altLabel',
    'file:///vocabulary/disease#synonym',
    'urn:example:synonym',
])
def test_attributes_option_with_iris(tmp_path, iri):
    """
    The IRIs of the 'attributes' option are taken as they are, whether they
    have an authority or not.
    """
    with open(VOCABULARY, encoding='utf-8') as f:
        text = f.read().replace('skos:altLabel', f'<{iri}>')
    vocabulary = tmp_path / 'disease_vocabulary.ttl'
    vocabulary.write_text(text, encoding='utf-8')

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}\n'
        f'attributes=skos:prefLabel,{iri}',
        mapping=mapping_copy(tmp_path, WITHOUT_ATTRIBUTE),
    ))

    assert_isomorphic(expected_graph(), g_morph)


@pytest.mark.parametrize('attribute', [
    # A prefix that is not well known.
    'skso:prefLabel',
    'prefLabel',
    # A prefix, not a prefixed name.
    'skos',
    f'<{SKOS}prefLabel>',
])
def test_attributes_option_not_an_iri(tmp_path, attribute):
    """An attribute that is not an IRI fails instead of indexing nothing."""
    error = f"Option 'attributes' of resource 'disease_vocabulary' .*'{re.escape(attribute)}'"
    with pytest.raises(ValueError, match=error):
        morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={VOCABULARY}\n'
            f'attributes=skos:altLabel,{attribute}',
            mapping=mapping_copy(tmp_path, WITHOUT_ATTRIBUTE),
        ))


def test_attributes_option_unused():
    """
    A mistake in the 'attributes' option does not stop a mapping whose
    executions all name their attributes, since none of them uses it.
    """
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}\n'
        f'attributes=skso:prefLabel'
    ))

    assert_isomorphic(expected_graph(), g_morph)


@pytest.mark.parametrize('attributes, expected', [
    # Without the option, every attribute of the vocabulary.
    ('', {('pid_00001', ATAXIA), ('pid_00002', NARCOLEPSY)}),
    (f'attributes={SKOS}prefLabel', {('pid_00002', NARCOLEPSY)}),
])
def test_executions_naming_different_attributes(attributes, expected):
    """
    Each execution matches against the attributes it names, or against those of
    the resource when it names none, whatever the other executions name.
    """
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}\n'
        f'{attributes}',
        mapping=os.path.join(TEST_DIR, 'mapping_two_executions.ttl'),
    ))

    assert reconciled(g_morph, 'https://example.org/kg/diseaseByAltLabel') == {('pid_00001', ATAXIA)}
    assert reconciled(g_morph, 'https://example.org/kg/disease') == expected


def test_execution_naming_no_attribute_next_to_one_naming_iris(tmp_path):
    """
    An execution naming no attribute matches against the literals of the
    vocabulary only, although another one names an attribute taking IRIs.
    """
    with open(VOCABULARY, encoding='utf-8') as f:
        text = f.read().replace(
            'skos:notation "ORPHA:352587"',
            'skos:notation "ORPHA:352587" ;\n'
            '    skos:exactMatch <http://www.orpha.net/ORDO/Orphanet_352587>',
        )
    vocabulary = tmp_path / 'disease_vocabulary.ttl'
    vocabulary.write_text(text, encoding='utf-8')
    patients = tmp_path / 'patients.csv'
    patients.write_text(
        'pid,disease_label\n'
        'pid_00001,http://www.orpha.net/ORDO/Orphanet_352587\n'
        'pid_00002,ADCA-DN\n',
        encoding='utf-8',
    )
    mapping = mapping_copy(
        tmp_path,
        ('test/rml-fnml/reconciliation/patients.csv', patients.as_posix()),
        ('rml:inputValue skos:altLabel', 'rml:inputValue skos:exactMatch'),
        mapping=os.path.join(TEST_DIR, 'mapping_two_executions.ttl'),
    )

    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={vocabulary}',
        mapping=mapping,
    ))

    assert reconciled(g_morph, 'https://example.org/kg/diseaseByAltLabel') == {('pid_00001', ATAXIA)}
    assert reconciled(g_morph, 'https://example.org/kg/disease') == {('pid_00002', NARCOLEPSY)}


def test_attributes_bound_under_both_parameter_iris(tmp_path):
    """
    The attributes an execution binds as grel:attributeIRI and as
    morph-fn:attributeIRI are all matched against.
    """
    mapping = mapping_copy(tmp_path, (
        'rml:parameter morph-fn:attributeIRI ;\n'
        '        rml:inputValue skos:prefLabel , skos:altLabel',
        'rml:parameter grel:attributeIRI ;\n'
        '        rml:inputValue skos:prefLabel\n'
        '    ] ;\n'
        '    rml:input [\n'
        '        rml:parameter morph-fn:attributeIRI ;\n'
        '        rml:inputValue skos:altLabel',
    ))
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=mapping,
    ))

    assert_isomorphic(expected_graph(), g_morph)


@pytest.mark.parametrize('replacements', [
    (),
    # Matched against every attribute of the vocabulary.
    (WITHOUT_ATTRIBUTE,),
])
def test_concept_schemes_and_collections_are_not_concepts(tmp_path, replacements):
    """
    A value labelling a concept scheme or a collection is not reconciled with
    it, while one labelling a concept is, even when concepts are not typed.
    """
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={os.path.join(TEST_DIR, "disease_vocabulary_schemes.ttl")}',
        mapping=mapping_copy(tmp_path, ('patients.csv', 'patients_schemes.csv'), *replacements),
    ))

    assert reconciled(g_morph) == {('pid_00001', ATAXIA)}


@pytest.mark.parametrize('other_iri', [
    '{OTHER_VOCABULARY_IRI}',
    # A positional placeholder, naming no variable.
    'https://example.org/vocabulary/{0}',
])
def test_vocabulary_iri_from_the_environment(monkeypatch, other_iri):
    """
    The IRI of a resource matches the mapping once its placeholders are
    replaced, and a resource whose placeholders cannot be is passed over.
    """
    monkeypatch.setenv('DISEASE_VOCABULARY_IRI', 'https://example.org/vocabulary/disease')
    monkeypatch.delenv('OTHER_VOCABULARY_IRI', raising=False)
    monkeypatch.delenv('OTHER_VOCABULARY_URL', raising=False)
    g_morph = morph_kgc.materialize(
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'[RESOURCE:other_vocabulary]\n'
        f'resource_type=SKOS_VOCABULARY\n'
        f'iri={other_iri}\n'
        f'url={{OTHER_VOCABULARY_URL}}\n'
        f'[RESOURCE:disease_vocabulary]\n'
        f'resource_type=SKOS_VOCABULARY\n'
        f'iri={{DISEASE_VOCABULARY_IRI}}\n'
        f'url={VOCABULARY}\n'
        f'[DataSource]\n'
        f'mappings={VOCABULARY_IRI_MAPPING}'
    )

    assert_isomorphic(expected_graph(), g_morph)


def test_vocabulary_iri_from_an_unset_variable(monkeypatch, caplog):
    """A resource whose IRI cannot be known is said to be passed over."""
    monkeypatch.delenv('DISEASE_VOCABULARY_IRI', raising=False)
    with pytest.raises(ValueError, match='not declared in the configuration'):
        morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'iri={{DISEASE_VOCABULARY_IRI}}\n'
            f'url={VOCABULARY}',
            mapping=VOCABULARY_IRI_MAPPING,
        ))

    assert "'DISEASE_VOCABULARY_IRI', which is not set" in caplog.text


def test_vocabulary_url_from_the_environment_is_not_kept(tmp_path, monkeypatch):
    """
    The URL read from the environment, which may hold a secret, is not written
    to the state directory.
    """
    from morph_kgc.functions import executor
    from morph_kgc.functions.state import get_context

    state_dir = tmp_path / 'state'
    contexts = []

    def spying_get_context(function_iri, run_config):
        # The contexts of the run, and the index files they refer to, are
        # removed when it ends: read them meanwhile.
        contexts.extend(path.read_bytes() for path in state_dir.glob('*/*') if path.is_file())
        return get_context(function_iri, run_config)

    monkeypatch.setattr(executor, 'get_context', spying_get_context)

    with VocabularyServer() as server:
        monkeypatch.setenv('DISEASE_VOCABULARY_URL', f'{server.url}/vocabulary?apikey=t0p-s3cr3t')
        g_morph = morph_kgc.materialize(
            f'[CONFIGURATION]\n'
            f'output_format=N-QUADS\n'
            # A single process, so that the spy sees the contexts being read.
            f'number_of_processes=1\n'
            f'state_dir={state_dir}\n'
            f'[RESOURCE:disease_vocabulary]\n'
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={{DISEASE_VOCABULARY_URL}}\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}\n'
            f'[DataSource]\n'
            f'mappings={MAPPING}'
        )

    assert_isomorphic(expected_graph(), g_morph)
    assert contexts
    for context in contexts:
        assert b't0p-s3cr3t' not in context


def test_vocabulary_referenced_by_its_url_from_the_environment(tmp_path, monkeypatch):
    """A mapping may name the vocabulary by its URL, read from the environment."""
    with VocabularyServer() as server:
        monkeypatch.setenv('DISEASE_VOCABULARY_URL', f'{server.url}/vocabulary')
        mapping = mapping_copy(
            tmp_path,
            ('https://example.org/vocabulary/disease', f'{server.url}/vocabulary'),
            mapping=VOCABULARY_IRI_MAPPING,
        )
        g_morph = morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={{DISEASE_VOCABULARY_URL}}\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}',
            mapping=mapping,
        ))

    assert_isomorphic(expected_graph(), g_morph)


def test_vocabulary_url_holding_credentials():
    """Credentials are declared as such, not in the URL, which is not echoed."""
    with VocabularyServer() as server:
        url = server.url.replace('://', f'://{USERNAME}:{PASSWORD}@')
        with pytest.raises(ValueError, match="'url' of resource 'disease_vocabulary' holds credentials") as error:
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={url}/vocabulary'
            ))

        assert server.requests == []

    assert PASSWORD not in str(error.value)


def test_undeclared_resource():
    """A mapping reconciling against an undeclared resource says so."""
    with pytest.raises(ValueError, match='not declared in the configuration'):
        morph_kgc.materialize(
            f'[CONFIGURATION]\n'
            f'output_format=N-QUADS\n'
            f'[DataSource]\n'
            f'mappings={MAPPING}'
        )


def test_wrong_resource_type():
    """Reconciling a vocabulary against a SPARQL endpoint resource is reported."""
    with pytest.raises(ValueError, match='resource_type'):
        morph_kgc.materialize(config(
            f'resource_type=SPARQL_ENDPOINT\n'
            f'url={VOCABULARY}'
        ))


# ── SPARQL endpoint ───────────────────────────────────────────────────────────

SPARQL_MAPPING = os.path.join(TEST_DIR, 'mapping_sparql.ttl')
# The endpoint the mappings name, as in the discussion that requested the function.
MAPPING_ENDPOINT = 'https://data.com/database/query'

STUDIES_QUERY = """
            PREFIX schema: <https://schema.org/>
            SELECT ?entity_iri ?matching_value_1 WHERE {
                GRAPH ?g {
                    ?entity_iri a schema:MedicalStudy ;
                                schema:identifier ?matching_value_1 .
                }
            }
        """

DRUGS_QUERY = """
            PREFIX schema: <https://schema.org/>
            SELECT ?entity_iri ?matching_value_1 WHERE {
                GRAPH ?g { ?entity_iri a schema:Drug ; schema:name ?matching_value_1 . }
            }
        """


def sparql_config(resources='', mapping=SPARQL_MAPPING, processes=1):
    return (
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'number_of_processes={processes}\n'
        f'{resources}\n'
        f'[DataSource]\n'
        f'mappings={mapping}'
    )


def endpoint_resource(url, extra_options='', name='clinical_trials', iri=MAPPING_ENDPOINT):
    """A resource declaring the credentials of the endpoint the mapping names."""
    return (
        f'[RESOURCE:{name}]\n'
        f'resource_type=SPARQL_ENDPOINT\n'
        f'{f"iri={iri}" if iri else ""}\n'
        f'url={url}\n'
        f'username={USERNAME}\n'
        f'password={PASSWORD}\n'
        f'{extra_options}'
    )


def sparql_mapping(tmp_path, *replacements, mapping=SPARQL_MAPPING):
    """A copy of *mapping* with each (old, new) replacement applied."""
    return mapping_copy(tmp_path, *replacements, mapping=mapping)


def expected_sparql_graph(file_name='output_sparql.nq'):
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, file_name))
    return g


@pytest.mark.parametrize('method', ['GET', 'POST'])
def test_reconcile_entity_over_sparql(method):
    """
    The query of the mapping is sent once, as written, with the credentials the
    configuration file declares for the endpoint, and its answer is reused for
    every value.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql', f'method={method}')
        ))

        assert server.requests == [('/sparql', STUDIES_QUERY)]
        assert server.methods == [method]

    assert_isomorphic(expected_sparql_graph(), g_morph)


def test_reconcile_mapping_of_the_discussion():
    """
    The mapping of the feature request, written in the legacy FNML vocabulary
    and naming the function with the morph-fn prefix, runs as written against a
    store whose default graph includes the named graphs.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql/union'),
            mapping=os.path.join(TEST_DIR, 'mapping_sparql_discussion.ttl'),
        ))

        assert [path for path, _ in server.requests] == ['/sparql/union']

    assert_isomorphic(expected_sparql_graph(), g_morph)


def test_query_without_graph_pattern_misses_the_named_graphs(caplog):
    """
    A query with no GRAPH pattern reads the default graph only, which on many
    stores leaves out the named graphs: no entity is found, and the log says why.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql'),
            mapping=os.path.join(TEST_DIR, 'mapping_sparql_discussion.ttl'),
        ))

    assert len(g_morph) == 0
    assert 'GRAPH ?g' in caplog.text


def test_endpoint_url_in_the_mapping_only(tmp_path):
    """An endpoint needing no credentials needs no configuration at all."""
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path, (MAPPING_ENDPOINT, f'{server.url}/public/sparql')
        )
        g_morph = morph_kgc.materialize(sparql_config(mapping=mapping))

        assert server.requests == [('/public/sparql', STUDIES_QUERY)]

    assert_isomorphic(expected_sparql_graph(), g_morph)


def test_endpoint_needing_undeclared_credentials(tmp_path):
    """An endpoint denying access says where its credentials are declared."""
    with VocabularyServer() as server:
        mapping = sparql_mapping(tmp_path, (MAPPING_ENDPOINT, f'{server.url}/sparql'))
        with pytest.raises(ValueError, match=r"'username' and 'password'.*\[RESOURCE:<name>\]"):
            morph_kgc.materialize(sparql_config(mapping=mapping))


def test_endpoint_matched_by_url_from_the_environment(tmp_path, monkeypatch):
    """The URL of a resource matches the mapping once its placeholders are replaced."""
    with VocabularyServer() as server:
        monkeypatch.setenv('CLINICAL_TRIALS_ENDPOINT', f'{server.url}/sparql')
        mapping = sparql_mapping(tmp_path, (MAPPING_ENDPOINT, f'{server.url}/sparql'))
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource('{CLINICAL_TRIALS_ENDPOINT}', iri=''),
            mapping=mapping,
        ))

    assert_isomorphic(expected_sparql_graph(), g_morph)


def test_endpoint_url_from_an_unset_variable(tmp_path, monkeypatch, caplog):
    """A resource whose URL cannot be known is said to be passed over."""
    monkeypatch.delenv('CLINICAL_TRIALS_ENDPOINT', raising=False)
    with VocabularyServer() as server:
        mapping = sparql_mapping(tmp_path, (MAPPING_ENDPOINT, f'{server.url}/sparql'))
        with pytest.raises(ValueError, match="'username' and 'password'"):
            morph_kgc.materialize(sparql_config(
                endpoint_resource('{CLINICAL_TRIALS_ENDPOINT}', iri=''),
                mapping=mapping,
            ))

    assert "'CLINICAL_TRIALS_ENDPOINT', which is not set" in caplog.text


def test_endpoint_matched_by_iri_from_the_environment(tmp_path, monkeypatch):
    """The IRI of a resource matches the mapping once its placeholders are replaced."""
    with VocabularyServer() as server:
        monkeypatch.setenv('CLINICAL_TRIALS_IRI', f'{server.url}/sparql')
        mapping = sparql_mapping(tmp_path, (MAPPING_ENDPOINT, f'{server.url}/sparql'))
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql/union', iri='{CLINICAL_TRIALS_IRI}'),
            mapping=mapping,
        ))

        assert [path for path, _ in server.requests] == ['/sparql/union']

    assert_isomorphic(expected_sparql_graph(), g_morph)


def test_endpoint_credentials_from_empty_variables(monkeypatch):
    """Credentials empty once read from the environment say where to declare them."""
    monkeypatch.setenv('CLINICAL_TRIALS_USER', '')
    monkeypatch.setenv('CLINICAL_TRIALS_PASSWORD', '')
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=r"'username' and 'password'.*\[RESOURCE:<name>\]"):
            morph_kgc.materialize(sparql_config(
                f'[RESOURCE:clinical_trials]\n'
                f'resource_type=SPARQL_ENDPOINT\n'
                f'iri={MAPPING_ENDPOINT}\n'
                f'url={server.url}/sparql\n'
                f'username={{CLINICAL_TRIALS_USER}}\n'
                f'password={{CLINICAL_TRIALS_PASSWORD}}\n'
            ))


def test_credentials_from_unset_variables(monkeypatch):
    """Credentials naming unset variables are no credentials, and do not fail."""
    monkeypatch.delenv('CLINICAL_TRIALS_USER', raising=False)
    monkeypatch.setenv('CLINICAL_TRIALS_PASSWORD', '')
    resource = ResourceConfig(
        name     = 'clinical_trials',
        username = '{CLINICAL_TRIALS_USER}',
        password = '{CLINICAL_TRIALS_PASSWORD}',
    )

    assert not resource.has_credentials()


def test_endpoint_declared_without_url():
    """A resource naming the endpoint by its IRI must say where to query it."""
    with pytest.raises(ValueError, match="does not declare the 'url'"):
        morph_kgc.materialize(sparql_config(
            f'[RESOURCE:clinical_trials]\n'
            f'resource_type=SPARQL_ENDPOINT\n'
            f'iri={MAPPING_ENDPOINT}\n'
        ))


def test_several_reconciliations_in_one_mapping():
    """
    Each execution pairs its query with its endpoint, so a mapping reconciles
    against several knowledge graphs. The same query sent to the same endpoint
    by two executions is sent once.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql') + (
                f'\n[RESOURCE:public]\n'
                f'resource_type=SPARQL_ENDPOINT\n'
                f'iri=https://data.com/public/query\n'
                f'url={server.url}/public/sparql\n'
            ),
            mapping=os.path.join(TEST_DIR, 'mapping_sparql_several.ttl'),
        ))

        assert sorted(server.requests) == sorted([
            ('/public/sparql', STUDIES_QUERY),
            ('/sparql', DRUGS_QUERY),
            ('/sparql', STUDIES_QUERY),
        ])

    assert_isomorphic(expected_sparql_graph('output_sparql_several.nq'), g_morph)


def test_value_matching_several_entities(tmp_path):
    """A value matching several entities yields one triple per entity."""
    query = (
        'SELECT ?entity_iri ("NCT07667218" AS ?matching_value_1) '
        'WHERE { GRAPH ?g { ?entity_iri a <https://schema.org/MedicalStudy> } }'
    )
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}/public/sparql'),
            (STUDIES_QUERY, query),
        )
        g_morph = morph_kgc.materialize(sparql_config(mapping=mapping))

    assert {str(o) for o in g_morph.objects()} == {
        f'https://data.com/id/{i}' for i in range(1, 5)
    }


def test_case_insensitive_matching_in_the_mapping(tmp_path):
    """
    Values are matched exactly by default, so that, rather than binding
    morph-fn:matching, a mapping may match them whatever their case by
    lowercasing both the values of the query and the values it reconciles.
    """
    with open(os.path.join(TEST_DIR, 'clinical_trials.csv'), encoding='utf-8') as f:
        rows = f.read().replace('NCT07667218', 'nct07667218').replace('NCT07667205', 'Nct07667205')
    (tmp_path / 'clinical_trials.csv').write_text(rows, encoding='utf-8')
    source = str(tmp_path / 'clinical_trials.csv').replace('\\', '/')

    lowercase_query = (
        'PREFIX schema: <https://schema.org/> '
        'SELECT ?entity_iri ?matching_value_1 WHERE { GRAPH ?g { '
        '?entity_iri a schema:MedicalStudy ; schema:identifier ?identifier } '
        'BIND(LCASE(?identifier) AS ?matching_value_1) }'
    )
    lowercase_value = (
        '[ rml:functionExecution <#LowerCase> ] ] .\n\n'
        '<#LowerCase>\n'
        '    rml:function grel:toLowerCase ;\n'
        '    rml:input [\n'
        '        rml:parameter grel:valueParameter ;\n'
        '        rml:inputValueMap [ rml:reference "clinical_trial_id" ]\n'
        '    ] .\n'
    )

    with VocabularyServer() as server:
        endpoint = (MAPPING_ENDPOINT, f'{server.url}/public/sparql')
        source_file = ('test/rml-fnml/reconciliation/clinical_trials.csv', source)
        exact = morph_kgc.materialize(sparql_config(
            mapping=sparql_mapping(tmp_path, endpoint, source_file),
        ))
        case_insensitive = morph_kgc.materialize(sparql_config(
            mapping=sparql_mapping(
                tmp_path, endpoint, source_file,
                (STUDIES_QUERY, lowercase_query),
                ('[ rml:reference "clinical_trial_id" ]\n    ] .\n', lowercase_value),
            ),
        ))

    assert len(exact) == 1
    assert_isomorphic(expected_sparql_graph(), case_insensitive)


def test_reconcile_entity_over_sparql_with_multiple_processes():
    """Worker processes read the indexes back from the state directory."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql'), processes=4,
        ))

    assert_isomorphic(expected_sparql_graph(), g_morph)


def test_reconcile_entity_over_sparql_from_yarrrml():
    """The query survives YARRRML as a block scalar, comments included."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql'),
            mapping=os.path.join(TEST_DIR, 'mapping_sparql.yarrrml'),
        ))

        assert '# The entities are described in named graphs.\n' in server.requests[0][1]

    assert_isomorphic(expected_sparql_graph(), g_morph)


@pytest.mark.parametrize('query, error', [
    (
        'SELECT ?study ?matching_value_1 WHERE { GRAPH ?g { ?study '
        '<https://schema.org/identifier> ?matching_value_1 } }',
        r'does not project \?entity_iri\. .* It projects: \?study \?matching_value_1',
    ),
    (
        'SELECT ?entity_iri ?identifier WHERE { GRAPH ?g { ?entity_iri '
        '<https://schema.org/identifier> ?identifier } }',
        r'does not project \?matching_value_1\.',
    ),
    (
        'SELECT ?entity_iri ?matching_value_1 ?matching_value_2 WHERE { GRAPH ?g { '
        '?entity_iri <https://schema.org/identifier> ?matching_value_1, ?matching_value_2 } }',
        # The mapping reconciles a single value.
        r'matches entities by 2 value\(s\), \?matching_value_1 \?matching_value_2, '
        r'but the mapping reconciles 1 value\(s\) with it\.',
    ),
])
def test_query_projecting_other_variables(tmp_path, query, error):
    """A query not following the variable convention fails instead of matching nothing."""
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}/public/sparql'),
            (STUDIES_QUERY, query),
        )
        with pytest.raises(ValueError, match=error):
            morph_kgc.materialize(sparql_config(mapping=mapping))


def test_query_projecting_the_graph_variable(tmp_path):
    """Variables projected besides the convention's, such as ?g, are ignored."""
    query = STUDIES_QUERY.replace(
        'SELECT ?entity_iri ?matching_value_1 WHERE', 'SELECT * WHERE'
    )
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}/public/sparql'),
            (STUDIES_QUERY, query),
        )
        g_morph = morph_kgc.materialize(sparql_config(mapping=mapping))

        assert server.requests == [('/public/sparql', query)]

    assert_isomorphic(expected_sparql_graph(), g_morph)


@pytest.mark.parametrize('replacement', [
    # No query at all.
    ('rml:parameter morph-fn:sparqlQuery ;', 'rml:parameter morph-fn:unused ;'),
    # A query read from the data, which is not known before it is read.
    (f'rml:inputValue """{STUDIES_QUERY}"""',
     'rml:inputValueMap [ rml:reference "clinical_trial_id" ]'),
])
def test_query_not_given_as_a_constant(tmp_path, replacement):
    """The query is sent before any data is read, so the mapping must give it."""
    mapping = sparql_mapping(tmp_path, replacement)
    with pytest.raises(ValueError, match='sparqlQuery.*once, to a constant'):
        morph_kgc.materialize(sparql_config(mapping=mapping))


@pytest.mark.parametrize('endpoint, error', [
    ('clinical_trials', r'must be an http\(s\) URL'),
    ('test/rml-fnml/reconciliation/clinical_trials.trig', r'must be an http\(s\) URL'),
    ('https://user:s3cr3t@data.com/database/query', 'holds credentials'),
])
def test_endpoint_not_a_plain_url(tmp_path, endpoint, error):
    """The endpoint is an http(s) URL, and its credentials stay out of the mapping."""
    mapping = sparql_mapping(tmp_path, (MAPPING_ENDPOINT, endpoint))
    with pytest.raises(ValueError, match=error):
        morph_kgc.materialize(sparql_config(mapping=mapping))


def test_query_bound_twice(tmp_path):
    """An execution sends one query, not one per binding."""
    mapping = sparql_mapping(tmp_path, (
        'rml:parameter morph-fn:sparqlQuery ;',
        'rml:parameter morph-fn:sparqlQuery ;\n        rml:inputValue "SELECT * {}" ;',
    ))
    with pytest.raises(ValueError, match='sparqlQuery.*once, to a constant'):
        morph_kgc.materialize(sparql_config(mapping=mapping))


def test_endpoint_declared_twice():
    """Two resources declaring the endpoint the mapping names are ambiguous."""
    with pytest.raises(ValueError, match='declared by several resources'):
        morph_kgc.materialize(sparql_config(
            endpoint_resource('https://staging.data.com/query')
            + '\n' + endpoint_resource('https://test.data.com/query', name='other')
        ))


def test_endpoint_rejecting_the_query(tmp_path):
    """What the endpoint says about a query it rejects is reported."""
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}/public/sparql'),
            (STUDIES_QUERY, 'SELECT ?entity_iri ?matching_value_1 WHERE { GRAPH ?g { '),
        )
        with pytest.raises(ValueError, match='answered 400 Bad Request: .+'):
            morph_kgc.materialize(sparql_config(mapping=mapping))


def test_query_that_is_not_a_select_query(tmp_path):
    """The answer to a query other than a SELECT query is not taken for one."""
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}/public/sparql'),
            (STUDIES_QUERY, 'ASK { GRAPH ?g { ?s ?p ?o } }'),
        )
        with pytest.raises(ValueError, match='must be a SELECT query'):
            morph_kgc.materialize(sparql_config(mapping=mapping))


def test_entities_that_are_not_iris(tmp_path, caplog):
    """Solutions binding the entity to something other than an IRI are left out."""
    query = (
        'PREFIX schema: <https://schema.org/> '
        'SELECT ?entity_iri ?matching_value_1 WHERE { GRAPH ?g { '
        '?study a schema:MedicalStudy ; schema:identifier ?matching_value_1 } '
        'BIND(IF(?study = <https://data.com/id/1>, STR(?study), ?study) AS ?entity_iri) }'
    )
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}/public/sparql'),
            (STUDIES_QUERY, query),
        )
        g_morph = morph_kgc.materialize(sparql_config(mapping=mapping))

    assert {str(o) for o in g_morph.objects()} == {
        'https://data.com/id/2', 'https://data.com/id/3',
    }
    assert 'other than an IRI in 1 solution(s)' in caplog.text


@pytest.mark.parametrize('variant, warning', [
    ('gzip', None),
    ('xml', None),
    ('no-head', 'cannot be read as a stream'),
    ('results-first', 'gives its results before its head'),
    ('bad-term', 'cannot be read as a stream'),
])
def test_answers_in_other_shapes(tmp_path, caplog, variant, warning):
    """
    An answer gzipped, in XML, in JSON without the head some endpoints leave
    out or with the results before it, or with a term the streaming parser
    rejects, is read too: the last three whole, which a warning says.
    """
    with VocabularyServer() as server:
        mapping = sparql_mapping(tmp_path, (MAPPING_ENDPOINT, f'{server.url}/public/sparql/{variant}'))
        g_morph = morph_kgc.materialize(sparql_config(mapping=mapping))

    assert_isomorphic(expected_sparql_graph(), g_morph)
    if warning:
        assert warning in caplog.text
    else:
        assert 'memory' not in caplog.text


def test_query_too_long_for_a_url(tmp_path):
    """A query too long to be sent in a URL is sent with POST."""
    long_query = STUDIES_QUERY + '#' * 5000 + '\n'
    with VocabularyServer() as server:
        endpoint = (MAPPING_ENDPOINT, f'{server.url}/public/sparql')
        morph_kgc.materialize(sparql_config(mapping=sparql_mapping(tmp_path, endpoint)))
        morph_kgc.materialize(sparql_config(
            mapping=sparql_mapping(tmp_path, endpoint, (STUDIES_QUERY, long_query)),
        ))

        assert server.methods == ['GET', 'POST']
        assert server.requests[1][1] == long_query


def test_endpoint_answering_too_late(tmp_path):
    """A query the endpoint answers too late says how to wait longer."""
    with VocabularyServer() as server:
        mapping = sparql_mapping(tmp_path, (MAPPING_ENDPOINT, f'{server.url}/public/sparql/slow'))
        with pytest.raises(ValueError, match="did not answer the query.*'timeout'"):
            morph_kgc.materialize(sparql_config(
                f'[RESOURCE:slow]\n'
                f'resource_type=SPARQL_ENDPOINT\n'
                f'url={server.url}/public/sparql/slow\n'
                f'timeout=1',
                mapping=mapping,
            ))


# ── Ambiguous values ──────────────────────────────────────────────────────────

AMBIGUOUS_VOCABULARY = os.path.join(TEST_DIR, 'disease_vocabulary_ambiguous.ttl')
# The concept of disease_vocabulary_ambiguous.ttl whose alternative label is
# the preferred label of NARCOLEPSY, which is the label of pid_00002.
ADCA_DN = 'https://example.org/vocabulary/disease/adca-dn'
AMBIGUOUS_LABEL = 'Autosomal dominant cerebellar ataxia-deafness-narcolepsy syndrome'


def ambiguity_error(ambiguity):
    """The error of an execution binding the ambiguity to an invalid value."""
    return re.escape(
        f"binds 'urn:morph:function:ambiguity' to '{ambiguity}', which is not valid. "
        "Must be one of: ALL, FAIL, NONE."
    )


def with_ambiguity(ambiguity):
    """The replacement adding an ambiguity input to the execution of mapping.ttl."""
    return (
        'rml:inputValue skos:prefLabel , skos:altLabel\n    ]',
        'rml:inputValue skos:prefLabel , skos:altLabel\n    ] ;\n'
        '    rml:input [\n'
        '        rml:parameter morph-fn:ambiguity ;\n'
        f'        rml:inputValue {ambiguity}\n'
        '    ]',
    )


def ambiguous_config(mapping=MAPPING, resource_options='', processes=1, configuration=''):
    """The configuration reconciling against disease_vocabulary_ambiguous.ttl."""
    return config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={AMBIGUOUS_VOCABULARY}\n'
        f'{resource_options}',
        mapping=mapping,
        processes=processes,
    ).replace('[CONFIGURATION]\n', f'[CONFIGURATION]\n{configuration}\n')


def multiple_match_reports(caplog, level='WARNING'):
    """The messages of the reports of ambiguous values logged at *level*."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelname == level and record.getMessage().startswith('MULTIPLE_MATCH')
    ]


def ambiguous_label_report(consequence, policy):
    """The report of the label of pid_00002, which matches two concepts."""
    return (
        f"MULTIPLE_MATCH: value '{AMBIGUOUS_LABEL}' matches 2 entities of resource "
        f"'disease_vocabulary': <{NARCOLEPSY}>, <{ADCA_DN}>. "
        f"{consequence}, as the ambiguity policy is {policy}."
    )


def terms(g_morph):
    """Every subject and object of *g_morph*."""
    return {str(term) for term in (*g_morph.subjects(), *g_morph.objects())}


@pytest.mark.parametrize('replacements, resource_options', [
    ((), ''),
    ((with_ambiguity('"ALL"'),), ''),
    ((), 'ambiguity=all'),
    # The execution chooses over the resource.
    ((with_ambiguity('"ALL"'),), 'ambiguity=NONE'),
])
def test_ambiguous_value_reconciled_to_every_concept_by_default(
    tmp_path, caplog, replacements, resource_options,
):
    """
    A value matching several concepts yields one triple per concept by default,
    and is only reported when debugging.
    """
    g_morph = morph_kgc.materialize(ambiguous_config(
        mapping_copy(tmp_path, *replacements), resource_options,
    ))

    assert reconciled(g_morph) == {
        ('pid_00001', ATAXIA), ('pid_00002', NARCOLEPSY), ('pid_00002', ADCA_DN),
    }
    assert 'MULTIPLE_MATCH' not in caplog.text


def test_ambiguous_value_reported_when_debugging(caplog):
    """With ALL, the report of an ambiguous value is a debug record."""
    morph_kgc.materialize(ambiguous_config(configuration='logging_level=DEBUG'))

    assert multiple_match_reports(caplog, 'DEBUG') == [
        ambiguous_label_report('A triple is generated for each of them', 'ALL'),
    ]
    assert multiple_match_reports(caplog) == []


@pytest.mark.parametrize('replacements, resource_options', [
    ((with_ambiguity('"NONE"'),), ''),
    ((with_ambiguity('"none"'),), ''),
    ((with_ambiguity('" None "'),), ''),
    ((), 'ambiguity=NONE'),
    ((), 'ambiguity= none '),
    # The execution chooses over the resource.
    ((with_ambiguity('"NONE"'),), 'ambiguity=FAIL'),
])
def test_ambiguity_none(tmp_path, caplog, replacements, resource_options):
    """
    With NONE, a value matching two concepts yields no triple, and is reported
    as a MULTIPLE_MATCH with both concepts, once, although three mapping rules
    reconcile it. The values matching a single concept are reconciled.
    """
    g_morph = morph_kgc.materialize(ambiguous_config(
        mapping_copy(tmp_path, *replacements), resource_options,
    ))

    assert reconciled(g_morph) == {('pid_00001', ATAXIA)}
    # Neither as the object of a patient, nor as the subject of a disease.
    assert NARCOLEPSY not in terms(g_morph) and ADCA_DN not in terms(g_morph)
    assert ATAXIA in {str(s) for s in g_morph.subjects()}
    assert multiple_match_reports(caplog) == [
        ambiguous_label_report('No triple is generated for it', 'NONE'),
    ]


def test_ambiguous_value_of_several_rows_reported_once(tmp_path, caplog):
    """An ambiguous value is reported once, however many rows hold it."""
    with open(os.path.join(TEST_DIR, 'patients.csv'), encoding='utf-8') as f:
        rows = f.read()
    rows += f'pid_00004,{AMBIGUOUS_LABEL}\npid_00005,ADCA-DN\n'
    (tmp_path / 'patients.csv').write_text(rows, encoding='utf-8')
    source = str(tmp_path / 'patients.csv').replace('\\', '/')

    g_morph = morph_kgc.materialize(ambiguous_config(
        mapping_copy(
            tmp_path,
            ('test/rml-fnml/reconciliation/patients.csv', source),
            with_ambiguity('"NONE"'),
        ),
    ))

    # ADCA-DN, an alternative label of a single concept, is not ambiguous.
    assert reconciled(g_morph) == {('pid_00001', ATAXIA), ('pid_00005', NARCOLEPSY)}
    assert multiple_match_reports(caplog) == [
        ambiguous_label_report('No triple is generated for it', 'NONE'),
    ]


@pytest.mark.parametrize('replacements, resource_options', [
    ((with_ambiguity('"FAIL"'),), ''),
    ((with_ambiguity('"fail"'),), 'ambiguity=NONE'),
    ((), 'ambiguity=FAIL'),
])
@pytest.mark.parametrize('processes', [1, 4])
def test_ambiguity_fail(tmp_path, replacements, resource_options, processes):
    """With FAIL, the first ambiguous value aborts the run, reporting it."""
    report = ambiguous_label_report('The run is aborted', 'FAIL')
    with pytest.raises(ValueError, match=re.escape(report)):
        morph_kgc.materialize(ambiguous_config(
            mapping_copy(tmp_path, *replacements), resource_options, processes,
        ))


@pytest.mark.parametrize('replacements, resource_options', [
    ((with_ambiguity('"FAIL"'),), ''),
    ((), 'ambiguity=FAIL'),
])
def test_ambiguity_fail_without_ambiguous_values(tmp_path, replacements, resource_options):
    """A run reconciling no ambiguous value is not aborted."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}\n'
        f'{resource_options}',
        mapping=mapping_copy(tmp_path, *replacements),
    ))

    assert_isomorphic(expected_graph(), g_morph)


def test_ambiguity_none_with_multiple_processes(tmp_path):
    """Every worker process applies the policy of the execution."""
    g_morph = morph_kgc.materialize(ambiguous_config(
        mapping_copy(tmp_path, with_ambiguity('"NONE"')), processes=4,
    ))

    assert reconciled(g_morph) == {('pid_00001', ATAXIA)}
    assert NARCOLEPSY not in terms(g_morph) and ADCA_DN not in terms(g_morph)


def test_ambiguity_with_case_insensitive_matching(tmp_path, caplog):
    """
    Values made ambiguous by matching them case-insensitively are ambiguous as
    well. Here in the legacy FNML vocabulary, whose constants are rr:constant
    value maps.
    """
    mapping = mapping_copy(
        tmp_path,
        ('fnml:valueMap [ rr:constant "CASE-INSENSITIVE" ]\n        ] .',
         'fnml:valueMap [ rr:constant "CASE-INSENSITIVE" ]\n        ],\n'
         '        [\n'
         '            fnml:parameter morph-fn:ambiguity ;\n'
         '            fnml:valueMap [ rr:constant "NONE" ]\n'
         '        ] .'),
        mapping=os.path.join(TEST_DIR, 'mapping_legacy_fnml.ttl'),
    )
    g_morph = morph_kgc.materialize(ambiguous_config(mapping))

    assert reconciled(g_morph) == {('pid_00001', ATAXIA)}
    assert multiple_match_reports(caplog) == [
        f"MULTIPLE_MATCH: value '{AMBIGUOUS_LABEL.upper()}' matches 2 entities of "
        f"resource 'disease_vocabulary': <{NARCOLEPSY}>, <{ADCA_DN}>. "
        f"No triple is generated for it, as the ambiguity policy is NONE.",
    ]


@pytest.mark.parametrize('parameter', [
    '                    - parameter: morph-fn:ambiguity\n'
    '                      value: NONE\n',
    '                    - [morph-fn:ambiguity, none]\n',
])
def test_ambiguity_in_yarrrml(tmp_path, parameter):
    """The ambiguity is a plain value in YARRRML, which no prefix expands."""
    mapping = mapping_copy(
        tmp_path,
        ('                      value: skos:altLabel\n',
         '                      value: skos:altLabel\n' + parameter),
        mapping=os.path.join(TEST_DIR, 'mapping.yarrrml'),
    )
    g_morph = morph_kgc.materialize(ambiguous_config(mapping))

    assert reconciled(g_morph) == {('pid_00001', ATAXIA)}


@pytest.mark.parametrize('ambiguity', ['FIRST', 'ANY', 'NO', 'ALL_MATCHES', ''])
def test_invalid_ambiguity(tmp_path, ambiguity):
    """
    An ambiguity that is not one of those known fails, naming them, before the
    vocabulary is downloaded.
    """
    mapping = mapping_copy(tmp_path, with_ambiguity(f'"{ambiguity}"'))
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=ambiguity_error(ambiguity)):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary\n'
                f'username={USERNAME}\n'
                f'password={PASSWORD}',
                mapping=mapping,
            ))

        assert server.requests == []


@pytest.mark.parametrize('binding', [
    'rml:inputValueMap [ rml:reference "disease_label" ]',
    'rml:inputValueMap [ rml:template "{disease_label}" ]',
    # A constant, but two of them.
    'rml:inputValue "NONE" , "FAIL"',
])
def test_ambiguity_not_given_as_a_single_constant(tmp_path, binding):
    """An ambiguity read from the data fails before the vocabulary is downloaded."""
    old, new = with_ambiguity('"NONE"')
    mapping = mapping_copy(
        tmp_path, (old, new.replace('rml:inputValue "NONE"', binding)),
    )
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=r"must bind 'urn:morph:function:ambiguity' at most once, to a constant"):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary\n'
                f'username={USERNAME}\n'
                f'password={PASSWORD}',
                mapping=mapping,
            ))

        assert server.requests == []


@pytest.mark.parametrize('ambiguity', ['FIRST', 'yes'])
def test_invalid_ambiguity_option(ambiguity):
    """
    An ambiguity option that is not one of those known fails, naming them,
    before the vocabulary is downloaded.
    """
    error = (
        f"Option 'ambiguity' of resource 'disease_vocabulary', which function "
        f"'urn:morph:function:reconciliation:reconcileVocabularyConcept' reconciles "
        f"against, is '{ambiguity}', which is not valid. Must be one of: ALL, FAIL, NONE."
    )
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=re.escape(error)):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary\n'
                f'username={USERNAME}\n'
                f'password={PASSWORD}\n'
                f'ambiguity={ambiguity}',
            ))

        assert server.requests == []


# The query of mapping_sparql.ttl, but with the identifier of the fourth study
# replaced by the one of the first, which then matches both.
AMBIGUOUS_STUDIES_QUERY = """
            PREFIX schema: <https://schema.org/>
            SELECT ?entity_iri ?matching_value_1 WHERE {
                GRAPH ?g {
                    ?entity_iri a schema:MedicalStudy ;
                                schema:identifier ?identifier .
                }
                BIND(IF(?identifier = "NCT07667179", "NCT07667218", ?identifier) AS ?matching_value_1)
            }
        """

# The reconciled studies of the patients of clinical_trials.csv.
UNAMBIGUOUS_STUDIES = {
    ('urn:id:p2', 'https://data.com/id/2'), ('urn:id:p2', 'https://data.com/id/3'),
}
AMBIGUOUS_STUDIES = {
    ('urn:id:p1', 'https://data.com/id/1'), ('urn:id:p1', 'https://data.com/id/4'),
}


def with_sparql_ambiguity(ambiguity):
    """The replacement adding an ambiguity input to the execution of mapping_sparql.ttl."""
    return (
        '[ rml:reference "clinical_trial_id" ]\n    ] .\n',
        '[ rml:reference "clinical_trial_id" ]\n    ] ;\n'
        '    rml:input [\n'
        '        rml:parameter morph-fn:ambiguity ;\n'
        f'        rml:inputValue {ambiguity}\n'
        '    ] .\n',
    )


def studies(g_morph):
    return {(str(s), str(o)) for s, o in g_morph.subject_objects()}


@pytest.mark.parametrize('replacements, resource_options, expected', [
    ((), '', UNAMBIGUOUS_STUDIES | AMBIGUOUS_STUDIES),
    ((), 'ambiguity=NONE', UNAMBIGUOUS_STUDIES),
    ((with_sparql_ambiguity('"none"'),), '', UNAMBIGUOUS_STUDIES),
    ((with_sparql_ambiguity('"ALL"'),), 'ambiguity=FAIL', UNAMBIGUOUS_STUDIES | AMBIGUOUS_STUDIES),
])
def test_ambiguity_over_sparql(tmp_path, caplog, replacements, resource_options, expected):
    """
    The policy for ambiguous values is the execution's, or else the one of the
    resource declaring the endpoint, which the mapping names by its IRI.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql', resource_options),
            mapping=sparql_mapping(
                tmp_path, (STUDIES_QUERY, AMBIGUOUS_STUDIES_QUERY), *replacements,
            ),
        ))

    assert studies(g_morph) == expected
    reports = multiple_match_reports(caplog)
    if expected == UNAMBIGUOUS_STUDIES:
        assert reports == [
            f"MULTIPLE_MATCH: value 'NCT07667218' matches 2 entities of SPARQL "
            f"endpoint '{MAPPING_ENDPOINT}': <https://data.com/id/1>, "
            f"<https://data.com/id/4>. No triple is generated for it, as the "
            f"ambiguity policy is NONE.",
        ]
    else:
        assert reports == []


@pytest.mark.parametrize('replacements, resource_options', [
    ((with_sparql_ambiguity('"FAIL"'),), ''),
    ((), 'ambiguity=FAIL'),
])
def test_ambiguity_fail_over_sparql(tmp_path, replacements, resource_options):
    """With FAIL, the first ambiguous value aborts the run, reporting it."""
    report = (
        f"MULTIPLE_MATCH: value 'NCT07667218' matches 2 entities of SPARQL "
        f"endpoint '{MAPPING_ENDPOINT}': <https://data.com/id/1>, "
        f"<https://data.com/id/4>. The run is aborted, as the ambiguity policy is FAIL."
    )
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=re.escape(report)):
            morph_kgc.materialize(sparql_config(
                endpoint_resource(f'{server.url}/sparql', resource_options),
                mapping=sparql_mapping(
                    tmp_path, (STUDIES_QUERY, AMBIGUOUS_STUDIES_QUERY), *replacements,
                ),
            ))


def test_invalid_ambiguity_over_sparql(tmp_path):
    """An invalid ambiguity fails before any query is sent."""
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=ambiguity_error('SOME')):
            morph_kgc.materialize(sparql_config(
                endpoint_resource(f'{server.url}/sparql'),
                mapping=sparql_mapping(tmp_path, with_sparql_ambiguity('"SOME"')),
            ))

        assert server.requests == []


def test_invalid_ambiguity_option_over_sparql():
    """An invalid ambiguity option of an endpoint fails before any query is sent."""
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=r"Option 'ambiguity' of resource 'clinical_trials'.*is 'SOME'"):
            morph_kgc.materialize(sparql_config(
                endpoint_resource(f'{server.url}/sparql', 'ambiguity=SOME'),
            ))

        assert server.requests == []


# ── Ambiguous values of several executions ────────────────────────────────────

TWO_EXECUTIONS_MAPPING = os.path.join(TEST_DIR, 'mapping_two_executions.ttl')
SEVERAL_SPARQL_MAPPING = os.path.join(TEST_DIR, 'mapping_sparql_several.ttl')
# The predicates of mapping_two_executions.ttl.
DISEASE_BY_ALT_LABEL = 'https://example.org/kg/diseaseByAltLabel'
DISEASE = 'https://example.org/kg/disease'
ALL_CONCEPTS = {('pid_00001', ATAXIA), ('pid_00002', NARCOLEPSY), ('pid_00002', ADCA_DN)}


def execution_input(parameter, value):
    """An rml:input binding *parameter* to the constant *value*, added to an execution."""
    return f' ;\n    rml:input [ rml:parameter {parameter} ; rml:inputValue {value} ]'


def two_executions(tmp_path, alt_label_inputs='', disease_inputs=''):
    """
    mapping_two_executions.ttl, whose first execution matches the preferred and
    the alternative labels, and whose second names no attribute, with the
    inputs given added to each.
    """
    return mapping_copy(
        tmp_path,
        ('rml:inputValue skos:altLabel\n    ] .',
         f'rml:inputValue skos:prefLabel , skos:altLabel\n    ]{alt_label_inputs} .'),
        ('[ rml:reference "disease_label" ]\n    ] .',
         f'[ rml:reference "disease_label" ]\n    ]{disease_inputs} .'),
        mapping=TWO_EXECUTIONS_MAPPING,
    )


def two_vocabularies_config(mapping, options_a='', options_b='', url_a=AMBIGUOUS_VOCABULARY,
                            url_b=AMBIGUOUS_VOCABULARY, configuration=''):
    """A configuration declaring two vocabularies, vocabulary_a and vocabulary_b."""
    return (
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'number_of_processes=1\n'
        f'{configuration}\n'
        f'[RESOURCE:vocabulary_a]\n'
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={url_a}\n'
        f'{options_a}\n'
        f'[RESOURCE:vocabulary_b]\n'
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={url_b}\n'
        f'{options_b}\n'
        f'[DataSource]\n'
        f'mappings={mapping}'
    )


@pytest.mark.parametrize('alt_label_policy, disease_policy', [('ALL', 'NONE'), ('NONE', 'ALL')])
def test_ambiguous_value_reported_under_each_policy(tmp_path, caplog, alt_label_policy, disease_policy):
    """
    A value two executions reconcile under different policies is reported
    under each, whichever mapping rule runs first.
    """
    mapping = two_executions(
        tmp_path,
        execution_input('morph-fn:ambiguity', f'"{alt_label_policy}"'),
        execution_input('morph-fn:ambiguity', f'"{disease_policy}"'),
    )
    g_morph = morph_kgc.materialize(ambiguous_config(mapping, configuration='logging_level=DEBUG'))

    policies = {DISEASE_BY_ALT_LABEL: alt_label_policy, DISEASE: disease_policy}
    for predicate, policy in policies.items():
        expected = ALL_CONCEPTS if policy == 'ALL' else {('pid_00001', ATAXIA)}
        assert reconciled(g_morph, predicate) == expected
    assert multiple_match_reports(caplog, 'DEBUG') == [
        ambiguous_label_report('A triple is generated for each of them', 'ALL'),
    ]
    assert multiple_match_reports(caplog) == [
        ambiguous_label_report('No triple is generated for it', 'NONE'),
    ]


def test_same_report_of_two_executions_written_once(tmp_path, caplog):
    """
    Executions looking a value up differently (here, against different
    attributes) that report it alike write a single record.
    """
    g_morph = morph_kgc.materialize(ambiguous_config(
        two_executions(tmp_path), resource_options='ambiguity=NONE',
    ))

    assert reconciled(g_morph, DISEASE_BY_ALT_LABEL) == {('pid_00001', ATAXIA)}
    assert reconciled(g_morph, DISEASE) == {('pid_00001', ATAXIA)}
    assert multiple_match_reports(caplog) == [
        ambiguous_label_report('No triple is generated for it', 'NONE'),
    ]


@pytest.mark.parametrize('options_a, options_b', [
    ('ambiguity=NONE', 'ambiguity=ALL'),
    ('ambiguity=ALL', 'ambiguity=NONE'),
    ('ambiguity=NONE', ''),
])
def test_ambiguity_option_of_each_resource(tmp_path, options_a, options_b):
    """Each resource sets the policy of the executions reconciling against it."""
    mapping = two_executions(
        tmp_path,
        execution_input('morph-fn:resource', '"vocabulary_a"'),
        execution_input('morph-fn:resource', '"vocabulary_b"'),
    )
    g_morph = morph_kgc.materialize(two_vocabularies_config(mapping, options_a, options_b))

    for predicate, options in ((DISEASE_BY_ALT_LABEL, options_a), (DISEASE, options_b)):
        expected = {('pid_00001', ATAXIA)} if options == 'ambiguity=NONE' else ALL_CONCEPTS
        assert reconciled(g_morph, predicate) == expected


def test_ambiguous_value_reported_for_each_resource(tmp_path, caplog):
    """A value ambiguous in two vocabularies is reported for each."""
    mapping = two_executions(
        tmp_path,
        execution_input('morph-fn:resource', '"vocabulary_a"'),
        execution_input('morph-fn:resource', '"vocabulary_b"'),
    )
    morph_kgc.materialize(two_vocabularies_config(mapping, 'ambiguity=NONE', 'ambiguity=NONE'))

    assert sorted(multiple_match_reports(caplog)) == [
        f"MULTIPLE_MATCH: value '{AMBIGUOUS_LABEL}' matches 2 entities of resource "
        f"'{name}': <{NARCOLEPSY}>, <{ADCA_DN}>. No triple is generated for it, "
        f"as the ambiguity policy is NONE."
        for name in ('vocabulary_a', 'vocabulary_b')
    ]


@pytest.mark.parametrize('invalid', ['vocabulary_a', 'vocabulary_b'])
def test_invalid_ambiguity_option_of_one_of_two_resources(tmp_path, invalid):
    """
    The ambiguity options of every resource are checked before any vocabulary
    is downloaded, whichever is invalid.
    """
    mapping = two_executions(
        tmp_path,
        execution_input('morph-fn:resource', '"vocabulary_a"'),
        execution_input('morph-fn:resource', '"vocabulary_b"'),
    )
    credentials = f'username={USERNAME}\npassword={PASSWORD}\n'
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=f"Option 'ambiguity' of resource '{invalid}'"):
            morph_kgc.materialize(two_vocabularies_config(
                mapping,
                credentials + ('ambiguity=FIRST' if invalid == 'vocabulary_a' else ''),
                credentials + ('ambiguity=FIRST' if invalid == 'vocabulary_b' else ''),
                url_a=f'{server.url}/vocabulary',
                url_b=f'{server.url}/vocabulary.nq',
            ))

        assert server.requests == []


def several_endpoints(server, clinical_trials_options='', public_options=''):
    """The endpoints of mapping_sparql_several.ttl, as test_several_reconciliations_in_one_mapping declares them."""
    return endpoint_resource(f'{server.url}/sparql', clinical_trials_options) + (
        f'\n[RESOURCE:public]\n'
        f'resource_type=SPARQL_ENDPOINT\n'
        f'iri=https://data.com/public/query\n'
        f'url={server.url}/public/sparql\n'
        f'{public_options}\n'
    )


def with_sparql_execution_input(execution, parameter, value):
    """The replacement adding an input to *execution* of mapping_sparql_several.ttl."""
    head = f'{execution}\n    rml:function morph-fn:reconcileEntityOverSPARQL ;\n'
    return (head, f'{head}    rml:input [ rml:parameter {parameter} ; rml:inputValue {value} ] ;\n')


SEVERAL_SPARQL_EXECUTIONS = [
    '<#ClinicalTrialReconciliation>',
    '<#ClinicalTrialReconciliationAgain>',
    '<#DrugReconciliation>',
    '<#PublicClinicalTrialReconciliation>',
]


def objects_of(g_morph, predicate):
    return {
        (str(s), str(o))
        for s, o in g_morph.subject_objects(URIRef(f'https://example.org/ontology/{predicate}'))
    }


@pytest.mark.parametrize('execution', SEVERAL_SPARQL_EXECUTIONS)
def test_invalid_ambiguity_of_one_of_several_sparql_executions(tmp_path, execution):
    """The ambiguity of every execution is checked before any query is sent."""
    mapping = sparql_mapping(
        tmp_path,
        with_sparql_execution_input(execution, 'morph-fn:ambiguity', '"SOME"'),
        mapping=SEVERAL_SPARQL_MAPPING,
    )
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=ambiguity_error('SOME')):
            morph_kgc.materialize(sparql_config(several_endpoints(server), mapping=mapping))

        assert server.requests == []


@pytest.mark.parametrize('invalid', ['clinical_trials', 'public'])
def test_invalid_ambiguity_option_of_one_of_several_endpoints(invalid):
    """The ambiguity options of every endpoint are checked before any query is sent."""
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=f"Option 'ambiguity' of resource '{invalid}'.*is 'SOME'"):
            morph_kgc.materialize(sparql_config(
                several_endpoints(
                    server,
                    'ambiguity=SOME' if invalid == 'clinical_trials' else '',
                    'ambiguity=SOME' if invalid == 'public' else '',
                ),
                mapping=SEVERAL_SPARQL_MAPPING,
            ))

        assert server.requests == []


@pytest.mark.parametrize('clinical_trials_policy, public_policy', [('NONE', 'ALL'), ('ALL', 'NONE')])
def test_ambiguity_option_of_each_endpoint(tmp_path, clinical_trials_policy, public_policy):
    """Each endpoint's resource sets the policy of the executions querying it."""
    mapping = sparql_mapping(
        tmp_path, (STUDIES_QUERY, AMBIGUOUS_STUDIES_QUERY), mapping=SEVERAL_SPARQL_MAPPING,
    )
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            several_endpoints(
                server, f'ambiguity={clinical_trials_policy}', f'ambiguity={public_policy}',
            ),
            mapping=mapping,
        ))

    def expected(policy):
        return UNAMBIGUOUS_STUDIES | (AMBIGUOUS_STUDIES if policy == 'ALL' else set())

    assert objects_of(g_morph, 'participatesIn') == expected(clinical_trials_policy)
    assert objects_of(g_morph, 'enrolledIn') == expected(clinical_trials_policy)
    assert objects_of(g_morph, 'registeredIn') == expected(public_policy)


@pytest.mark.parametrize('first_policy, again_policy', [('ALL', 'NONE'), ('NONE', 'ALL')])
def test_ambiguous_value_over_sparql_reported_under_each_policy(
    tmp_path, caplog, first_policy, again_policy,
):
    """
    A value two executions sending the same query reconcile under different
    policies is reported under each.
    """
    mapping = sparql_mapping(
        tmp_path,
        (STUDIES_QUERY, AMBIGUOUS_STUDIES_QUERY),
        with_sparql_execution_input(
            '<#ClinicalTrialReconciliation>', 'morph-fn:ambiguity', f'"{first_policy}"',
        ),
        with_sparql_execution_input(
            '<#ClinicalTrialReconciliationAgain>', 'morph-fn:ambiguity', f'"{again_policy}"',
        ),
        mapping=SEVERAL_SPARQL_MAPPING,
    )
    with VocabularyServer() as server:
        morph_kgc.materialize(sparql_config(several_endpoints(server), mapping=mapping).replace(
            '[CONFIGURATION]\n', '[CONFIGURATION]\nlogging_level=DEBUG\n',
        ))

    report = (
        f"MULTIPLE_MATCH: value 'NCT07667218' matches 2 entities of SPARQL "
        f"endpoint '{MAPPING_ENDPOINT}': <https://data.com/id/1>, <https://data.com/id/4>. "
    )
    assert multiple_match_reports(caplog) == [
        report + 'No triple is generated for it, as the ambiguity policy is NONE.',
    ]
    # The public endpoint, under ALL as well, is reported in a record of its own.
    assert report + 'A triple is generated for each of them, as the ambiguity policy is ALL.' \
        in multiple_match_reports(caplog, 'DEBUG')


# ── Entities matched by several values ────────────────────────────────────────

CODES_MAPPING = os.path.join(TEST_DIR, 'mapping_sparql_codes.ttl')
CODES_QUERY = """
            PREFIX schema: <https://schema.org/>
            SELECT ?entity_iri ?matching_value_1 ?matching_value_2 WHERE {
                GRAPH ?g {
                    ?entity_iri a schema:MedicalCode ;
                                schema:codeValue ?matching_value_1 ;
                                schema:codingSystem ?matching_value_2 .
                }
            }
        """
# The values of mapping_sparql_codes.ttl, as it binds them.
CODE_INPUT = (
    '    rml:input [\n'
    '        rml:parameter grel:valueParam1 ;\n'
    '        rml:inputValueMap [ rml:reference "code" ]\n'
    '    ]'
)
SCHEME_INPUT = (
    '    rml:input [\n'
    '        rml:parameter grel:valueParam2 ;\n'
    '        rml:inputValueMap [ rml:reference "scheme" ]\n'
    '    ]'
)
# The codes each patient of diagnoses.csv is diagnosed with.
DIAGNOSES = {
    ('urn:id:p1', 'https://data.com/id/6'),
    ('urn:id:p2', 'https://data.com/id/7'),
    ('urn:id:p3', 'https://data.com/id/8'),
    ('urn:id:p4', 'https://data.com/id/9'),
    ('urn:id:p5', 'https://data.com/id/10'),
}


def codes_mapping(tmp_path, server, *replacements, path='/public/sparql'):
    """A copy of mapping_sparql_codes.ttl querying *path* of *server*."""
    return sparql_mapping(
        tmp_path, (MAPPING_ENDPOINT, f'{server.url}{path}'), *replacements, mapping=CODES_MAPPING,
    )


def diagnoses(g_morph):
    return {(str(s), str(o)) for s, o in g_morph.subject_objects(URIRef(
        'https://example.org/ontology/diagnosedWith'
    ))}


def test_reconcile_entities_by_two_values():
    """
    Rows are reconciled with the entities matched by both their code and their
    coding system: values holding ',', '|' and whitespace are compared one by
    one, so that the codes of 'A|1' in 'ICD-10' and 'A' in '1|ICD-10', or of
    'B, C' in 'SNOMED CT' and 'B' in ' C,SNOMED CT', are told apart. The
    query is sent once.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql'), mapping=CODES_MAPPING,
        ))

        assert server.requests == [('/sparql', CODES_QUERY)]

    assert_isomorphic(expected_sparql_graph('output_sparql_codes.nq'), g_morph)


@pytest.mark.parametrize('path, expected', [
    ('/public/sparql/no-head', DIAGNOSES),
    ('/public/sparql/results-first', DIAGNOSES),
    ('/public/sparql/gzip', DIAGNOSES),
    # Read in memory, as the streaming parser rejects a solution, which binds
    # a single value and is left out.
    ('/public/sparql/bad-term', DIAGNOSES),
    # The coding system ' C,SNOMED CT' starts with a space, which an answer in
    # XML keeps, unescaped, as rdflib writes it.
    ('/public/sparql/xml', DIAGNOSES),
])
def test_reconcile_entities_by_two_values_from_answers_in_other_shapes(tmp_path, path, expected):
    """Every way of reading an answer reads all the values of its solutions."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(tmp_path, server, path=path),
        ))

    assert diagnoses(g_morph) == expected


def test_reconcile_entities_by_two_values_with_multiple_processes():
    """Worker processes look the tuples of values up in the index on disk."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql'), mapping=CODES_MAPPING, processes=4,
        ))

    assert diagnoses(g_morph) == DIAGNOSES


@pytest.mark.parametrize('replacements', [
    (('grel:valueParam1', 'grel:valueParam'),),
    (('grel:valueParam1', 'grel:valueParameter1'), ('grel:valueParam2', 'grel:valueParameter2')),
    (('grel:valueParam1', 'grel:valueParameter'),),
    # The inputs of an execution have no order.
    ((f'{CODE_INPUT} ;\n{SCHEME_INPUT}', f'{SCHEME_INPUT} ;\n{CODE_INPUT}'),),
])
def test_value_parameters_numbered_under_any_spelling(tmp_path, replacements):
    """
    The values are matched to the variables by the number of their parameter,
    under either GREL spelling, the first of them unnumbered or not.
    """
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(tmp_path, server, *replacements),
        ))

    assert diagnoses(g_morph) == DIAGNOSES


def test_values_matched_by_position(tmp_path):
    """The code bound as the second value is matched against coding systems."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(
                tmp_path, server,
                ('rml:reference "code"', 'rml:reference "SCHEME"'),
                ('rml:reference "scheme"', 'rml:reference "code"'),
                ('rml:reference "SCHEME"', 'rml:reference "scheme"'),
            ),
        ))

    assert len(g_morph) == 0


def test_constant_value(tmp_path):
    """A value may be a constant, such as the coding system all rows are in."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(
                tmp_path, server,
                ('rml:inputValueMap [ rml:reference "scheme" ]', 'rml:inputValue "ICD-10"'),
            ),
        ))

    # The coding system of every row is ICD-10, even when its scheme is empty.
    assert diagnoses(g_morph) == {
        (f'urn:id:{patient}', 'https://data.com/id/6') for patient in ('p1', 'p2', 'p6', 'p7')
    }


@pytest.mark.parametrize('third_value, expected', [('"Diagnosis"', DIAGNOSES), ('"Procedure"', set())])
def test_reconcile_entities_by_three_values(tmp_path, third_value, expected):
    """Entities are matched by any number of values."""
    query = CODES_QUERY.replace(
        '?matching_value_2 WHERE {', '?matching_value_2 ?matching_value_3 WHERE {',
    ).replace(
        'schema:codingSystem ?matching_value_2 .\n                }',
        'schema:codingSystem ?matching_value_2 .\n                }\n'
        '                BIND("Diagnosis" AS ?matching_value_3)',
    )
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(
                tmp_path, server,
                (CODES_QUERY, query),
                (SCHEME_INPUT, SCHEME_INPUT + ' ;\n'
                 '    rml:input [ rml:parameter grel:valueParam3 ; '
                 f'rml:inputValue {third_value} ]'),
            ),
        ))

    assert diagnoses(g_morph) == expected


def test_reconcile_entities_by_ten_values(tmp_path):
    """Values are told apart by the number of their position, 10 coming after 9."""
    query = CODES_QUERY.replace(
        '?matching_value_2 WHERE {',
        '?matching_value_2 '
        + ' '.join(f'?matching_value_{position}' for position in range(3, 11)) + ' WHERE {',
    ).replace(
        'schema:codingSystem ?matching_value_2 .\n                }',
        'schema:codingSystem ?matching_value_2 .\n                }\n'
        + ''.join(
            f'                BIND("v{position}" AS ?matching_value_{position})\n'
            for position in range(3, 11)
        ),
    )
    inputs = ''.join(
        f' ;\n    rml:input [ rml:parameter grel:valueParam{position} ; '
        f'rml:inputValue "v{position}" ]'
        for position in range(3, 11)
    )
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(tmp_path, server, (CODES_QUERY, query), (SCHEME_INPUT, SCHEME_INPUT + inputs)),
        ))

    assert diagnoses(g_morph) == DIAGNOSES


def test_reconcile_by_one_value_and_by_two_in_one_mapping():
    """Each query matches entities by the number of values its executions reconcile."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql'), mapping=f'{SPARQL_MAPPING},{CODES_MAPPING}',
        ))

        assert sorted(server.requests) == sorted([('/sparql', STUDIES_QUERY), ('/sparql', CODES_QUERY)])

    # Parsed at once: each parse of a Dataset puts its triples in a graph of its own.
    data = ''
    for file_name in ('output_sparql.nq', 'output_sparql_codes.nq'):
        with open(os.path.join(TEST_DIR, file_name), encoding='utf-8') as f:
            data += f.read()
    expected = Dataset()
    expected.parse(data=data, format='nquads')
    assert_isomorphic(expected, g_morph)


def test_reconcile_entities_by_two_values_from_yarrrml(tmp_path):
    """The values are numbered parameters in YARRRML as well."""
    mapping = tmp_path / 'mapping.yarrrml'
    with VocabularyServer() as server:
        mapping.write_text(
            'prefixes:\n'
            '    grel: http://users.ugent.be/~bjdmeest/function/grel.ttl#\n'
            '    morph-fn: "urn:morph:function:"\n'
            '    bi: https://example.org/ontology/\n'
            'mappings:\n'
            '    diagnoses:\n'
            '        sources:\n'
            '            - access: test/rml-fnml/reconciliation/diagnoses.csv\n'
            '              referenceFormulation: csv\n'
            '        subjects: urn:id:$(patient_id)\n'
            '        predicateobjects:\n'
            '            - p: bi:diagnosedWith\n'
            '              o:\n'
            '                function: morph-fn:reconcileEntityOverSPARQL\n'
            '                type: iri\n'
            '                parameters:\n'
            '                    - parameter: morph-fn:sparqlEndpointUrl\n'
            f'                      value: {server.url}/public/sparql\n'
            '                    - parameter: morph-fn:sparqlQuery\n'
            '                      value: |\n'
            + ''.join(f'                        {line.strip()}\n' for line in CODES_QUERY.strip().splitlines())
            + '                    - [grel:valueParam2, $(scheme)]\n'
            '                    - [grel:valueParam1, $(code)]\n',
            encoding='utf-8',
        )
        g_morph = morph_kgc.materialize(sparql_config(mapping=str(mapping)))

    assert diagnoses(g_morph) == DIAGNOSES


def test_entity_missing_a_value_is_not_matched(tmp_path):
    """An entity a solution binds no coding system to is matched by no row."""
    query = CODES_QUERY.replace(
        'schema:codingSystem ?matching_value_2 .',
        '.\n                    OPTIONAL { ?entity_iri schema:codingSystem ?matching_value_2 '
        'FILTER(?matching_value_2 != "ICD 10") }',
    ).replace('schema:codeValue ?matching_value_1 ;', 'schema:codeValue ?matching_value_1')
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(tmp_path, server, (CODES_QUERY, query)),
        ))

    assert diagnoses(g_morph) == DIAGNOSES - {('urn:id:p2', 'https://data.com/id/7')}


def test_ambiguous_tuple_of_values(tmp_path, caplog):
    """A tuple of values matching several entities is ambiguous as a whole."""
    # 'ICD 10' read as 'ICD-10' makes 'A|1' in 'ICD-10' match the codes of 6 and 7.
    query = CODES_QUERY.replace(
        'schema:codingSystem ?matching_value_2 .\n                }',
        'schema:codingSystem ?system .\n                }\n'
        '                BIND(IF(?system = "ICD 10", "ICD-10", ?system) AS ?matching_value_2)',
    )
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql', 'ambiguity=NONE'),
            mapping=sparql_mapping(tmp_path, (CODES_QUERY, query), mapping=CODES_MAPPING),
        ))

    assert diagnoses(g_morph) == DIAGNOSES - {
        ('urn:id:p1', 'https://data.com/id/6'), ('urn:id:p2', 'https://data.com/id/7'),
    }
    assert multiple_match_reports(caplog) == [
        f"MULTIPLE_MATCH: value ('A|1', 'ICD-10') matches 2 entities of SPARQL endpoint "
        f"'{MAPPING_ENDPOINT}': <https://data.com/id/6>, <https://data.com/id/7>. "
        f"No triple is generated for it, as the ambiguity policy is NONE.",
    ]


@pytest.mark.parametrize('path', ['/public/sparql', '/public/sparql/no-head'])
def test_query_matching_by_more_values_than_the_mapping(tmp_path, path):
    """
    A query matching entities by a value the mapping does not reconcile fails,
    even when its answer leaves its head out, instead of matching rows by some
    of the values of entities only.
    """
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=(
            r'(matches entities by 2 value\(s\), \?matching_value_1 \?matching_value_2, but '
            r'the mapping reconciles 1 value\(s\) with it|binds \?matching_value_2, so its '
            r'query matches entities by values the mapping does not reconcile: it reconciles '
            r'1 value\(s\) with it, \?matching_value_1)\. Bind one value to reconcile per variable'
        )):
            morph_kgc.materialize(sparql_config(
                mapping=codes_mapping(tmp_path, server, (f' ;\n{SCHEME_INPUT}', ''), path=path),
            ))


def test_query_matching_by_fewer_values_than_the_mapping(tmp_path):
    """A query projecting fewer values than the mapping binds fails, saying how many."""
    query = CODES_QUERY.replace(' ?matching_value_2 WHERE', ' WHERE').replace(
        'schema:codingSystem ?matching_value_2', 'schema:codingSystem ?system',
    )
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=(
            r'matches entities by 1 value\(s\), \?matching_value_1, but the mapping '
            r"reconciles 2 value\(s\) with it\. Bind one value to reconcile per variable"
        )):
            morph_kgc.materialize(sparql_config(
                mapping=codes_mapping(tmp_path, server, (CODES_QUERY, query)),
            ))


@pytest.mark.parametrize('variable, error', [
    ('matching_value_3', r'projects \?matching_value_3 but not \?matching_value_2: the values '
                         r'an entity is matched by are numbered from 1, without gaps\.'),
    ('matching_value_02', r'projects \?matching_value_02, which is not numbered as the values '
                          r'an entity is matched by are: \?matching_value_1, '
                          r'\?matching_value_2, \?matching_value_3\.\.\. from 1, without '
                          r'leading zeros\.'),
    ('matching_value_0', r'projects \?matching_value_0, which is not numbered'),
])
def test_query_projecting_values_not_numbered_from_1(tmp_path, variable, error):
    """The variables of the values are numbered from 1, without gaps or leading zeros."""
    query = CODES_QUERY.replace('?matching_value_2', f'?{variable}')
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=error):
            morph_kgc.materialize(sparql_config(
                mapping=codes_mapping(tmp_path, server, (CODES_QUERY, query)),
            ))


@pytest.mark.parametrize('replacements, error', [
    # No value at all.
    (((f'{CODE_INPUT} ;\n{SCHEME_INPUT}', '    rml:input [ rml:parameter morph-fn:unused ; '
       'rml:inputValue "x" ]'),),
     r"binds no value to reconcile\. Bind it to "
     r"'http://users\.ugent\.be/~bjdmeest/function/grel\.ttl#valueParam'\."),
    # A gap.
    ((('grel:valueParam2', 'grel:valueParam3'),),
     r'binds value 3 to reconcile but not value 2\. The values are numbered from 1'),
    # A number with a leading zero numbers no value.
    ((('grel:valueParam1 ;', 'grel:valueParam01 ;'),),
     r'binds value 2 to reconcile but not value 1\.'),
    # The first value twice, numbered and not.
    (((SCHEME_INPUT, SCHEME_INPUT + ' ;\n'
       '    rml:input [ rml:parameter grel:valueParam ; rml:inputValue "A|1" ]'),),
     r"binds value 1 to reconcile 2 times, to "
     r"'http://users\.ugent\.be/~bjdmeest/function/grel\.ttl#valueParam', "
     r"'http://users\.ugent\.be/~bjdmeest/function/grel\.ttl#valueParam1'\. Bind it once"),
    # The second value twice, under the same parameter.
    ((('rml:inputValueMap [ rml:reference "scheme" ]',
       'rml:inputValueMap [ rml:reference "scheme" ], [ rml:reference "code" ]'),),
     r'binds value 2 to reconcile 2 times'),
])
def test_values_not_bound_once_each_from_1(tmp_path, replacements, error):
    """
    Values bound other than once each, numbered from 1, fail before the query
    is sent.
    """
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=error):
            morph_kgc.materialize(sparql_config(
                mapping=codes_mapping(tmp_path, server, *replacements),
            ))

        assert server.requests == []


def test_same_query_reconciling_different_numbers_of_values(tmp_path):
    """
    Executions sending the same query to the same endpoint reconcile as many
    values, the number of those it projects, which fails before it is sent.
    """
    mapping = sparql_mapping(
        tmp_path,
        with_sparql_execution_input(
            '<#ClinicalTrialReconciliationAgain>', 'grel:valueParam2', '"ICD-10"',
        ),
        mapping=SEVERAL_SPARQL_MAPPING,
    )
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=(
            r"Executions '.*' and '.*' of function "
            r"'urn:morph:function:reconcileEntityOverSPARQL' send the same query to "
            r"'https://data\.com/database/query', but reconcile (1 and 2|2 and 1) values with it\."
        )):
            morph_kgc.materialize(sparql_config(several_endpoints(server), mapping=mapping))

        assert server.requests == []


def test_vocabulary_concepts_matched_by_the_first_value_numbered(tmp_path):
    """The value reconciled against a vocabulary may be numbered as the first."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=mapping_copy(tmp_path, ('grel:valueParam ;', 'grel:valueParameter1 ;')),
    ))

    assert_isomorphic(expected_graph(), g_morph)


def test_vocabulary_concepts_matched_by_two_values(tmp_path):
    """The concepts of a vocabulary are matched by a single value, before it is downloaded."""
    mapping = mapping_copy(tmp_path, (
        'rml:inputValue skos:prefLabel , skos:altLabel\n    ]',
        'rml:inputValue skos:prefLabel , skos:altLabel\n    ]'
        + execution_input('grel:valueParam2', '"ICD-10"'),
    ))
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match=(
            r"binds 2 values to reconcile, but the concepts of a vocabulary are matched by "
            r"a single value\."
        )):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary\n'
                f'username={USERNAME}\n'
                f'password={PASSWORD}',
                mapping=mapping,
            ))

        assert server.requests == []


# ── Ways of matching values ───────────────────────────────────────────────────

MATCHING_VOCABULARY = os.path.join(TEST_DIR, 'matching_vocabulary.ttl')
TERM = 'https://example.org/vocabulary/term/'
# What 'Café ', 'Straße' and 'STRASSE' (patients_matching.csv) match, by way of
# matching: lower-cased, as LOWERCASE matches them, each matches a single
# concept, which matching them case-insensitively makes ambiguous.
MATCHED_TERMS = {
    'EXACT': set(),
    'LOWERCASE': {('pid_00001', f'{TERM}1'), ('pid_00002', f'{TERM}7'), ('pid_00003', f'{TERM}6')},
    'CASE-INSENSITIVE': {
        *(('pid_00001', f'{TERM}{term}') for term in (1, 3, 4, 5)),
        *((pid, f'{TERM}{term}') for pid in ('pid_00002', 'pid_00003') for term in (6, 7)),
    },
}


def test_normalize_by_matching():
    """
    LOWERCASE lower-cases with str.lower(), and does nothing else;
    CASE-INSENSITIVE normalizes Unicode, collapses whitespace and case-folds.
    """
    from morph_kgc.reconciliation.index import normalize

    def lowercase(value):
        return normalize(value, 'LOWERCASE')

    assert lowercase('Café ') == lowercase('café ') == 'café '
    assert lowercase('Café ') not in (lowercase('cafe'), lowercase('Café'), lowercase('café '))
    assert lowercase('Straße') == 'straße' != lowercase('strasse')
    # As str.lower() does.
    assert lowercase('İSTANBUL') == 'i\u0307stanbul'
    assert lowercase('ΟΔΟΣ ΣΟΦΙΑΣ') == 'οδος σοφιας'
    assert lowercase(' Ⅻ  ﬁ\t') == ' ⅻ  ﬁ\t'

    def case_insensitive(value):
        return normalize(value, 'CASE-INSENSITIVE')

    assert case_insensitive('Café ') == case_insensitive('café  ') == 'café'
    assert case_insensitive('Straße') == case_insensitive('STRASSE') == 'strasse'
    # Values LOWERCASE matches, it matches as well.
    assert case_insensitive('İSTANBUL') == case_insensitive('i\u0307stanbul')

    assert normalize('Café ', 'EXACT') == 'Café '


@pytest.mark.parametrize('matching', ['EXACT', 'LOWERCASE', 'CASE-INSENSITIVE'])
def test_ways_of_matching_against_a_vocabulary(tmp_path, matching):
    """
    LOWERCASE matches 'Café ' to 'café ' only, not to 'cafe', 'Café', its
    decomposed form or 'café  ', and 'Straße' to 'straße', not to 'strasse'.
    """
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={MATCHING_VOCABULARY}',
        mapping=mapping_copy(
            tmp_path,
            ('test/rml-fnml/reconciliation/patients.csv',
             'test/rml-fnml/reconciliation/patients_matching.csv'),
            with_matching(f'"{matching}"'),
        ),
    ))

    assert reconciled(g_morph) == MATCHED_TERMS[matching]


@pytest.mark.parametrize('matching', ['"LOWERCASE"', '"lowercase"', '" Lowercase "'])
def test_reconcile_lowercase_matching(tmp_path, matching):
    """Lower-case matching, in any case, reconciles values that differ in case only."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=mapping_copy(tmp_path, with_matching(matching), mapping=UPPERCASE_MAPPING),
    ))

    assert_isomorphic(reconciled_graph(), g_morph)


def test_executions_matching_three_ways_download_the_vocabulary_once(tmp_path):
    """Executions matching a vocabulary in each way share a single download of it."""
    executions = ''.join(
        f'\n<#{matching.title().replace("-", "")}Execution>\n'
        '    rml:function morph-fr:reconcileVocabularyConcept ;\n'
        '    rml:input [ rml:parameter grel:valueParam ; '
        'rml:inputValueMap [ rml:reference "disease_label" ] ] ;\n'
        f'    rml:input [ rml:parameter morph-fn:matching ; rml:inputValue "{matching}" ] .\n'
        for matching in MATCHED_TERMS
    )
    maps = ''.join(
        f'    rml:predicateObjectMap [\n'
        f'        rml:predicate <https://example.org/kg/{matching}> ;\n'
        f'        rml:objectMap [ rml:functionExecution <#{matching.title().replace("-", "")}Execution> ; '
        f'rml:termType rml:IRI ]\n'
        f'    ] ;\n'
        for matching in MATCHED_TERMS
    )
    mapping = tmp_path / 'mapping.ttl'
    mapping.write_text(
        '@prefix rml: <http://w3id.org/rml/> .\n'
        '@prefix grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#> .\n'
        '@prefix morph-fr: <urn:morph:function:reconciliation:> .\n'
        '@prefix morph-fn: <urn:morph:function:> .\n'
        '@base <http://example.com/base/> .\n\n'
        '<#PatientsMapping>\n'
        '    a rml:TriplesMap ;\n'
        '    rml:logicalSource [\n'
        '        rml:source "test/rml-fnml/reconciliation/patients_matching.csv" ;\n'
        '        rml:referenceFormulation rml:CSV\n'
        '    ] ;\n'
        + maps +
        '    rml:subjectMap [ rml:template "https://example.org/kg/patient/{pid}" ] .\n'
        + executions,
        encoding='utf-8',
    )
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={server.url}/matching-vocabulary\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}',
            mapping=str(mapping),
        ))

        assert [path for path, _ in server.requests] == ['/matching-vocabulary']

    for matching, expected in MATCHED_TERMS.items():
        assert reconciled(g_morph, f'https://example.org/kg/{matching}') == expected


# The labels of matching_vocabulary.ttl, answered by a query.
MATCHING_QUERY = (
    'SELECT ?entity_iri ?matching_value_1 WHERE { VALUES (?entity_iri ?matching_value_1) { '
    + ' '.join(
        f'(<{TERM}{term}> "{label}")'
        for term, label in enumerate(
            ['café ', 'cafe', 'Café', 'café ', 'café  ', 'strasse', 'straße'], 1,
        )
    )
    + ' } }'
)


def with_sparql_input(parameter, value):
    """The replacement adding an input to the execution of mapping_sparql.ttl."""
    return (
        '[ rml:reference "clinical_trial_id" ]\n    ] .\n',
        '[ rml:reference "clinical_trial_id" ]\n    ] ;\n'
        '    rml:input [\n'
        f'        rml:parameter {parameter} ;\n'
        f'        rml:inputValue {value}\n'
        '    ] .\n',
    )


def patients_matching_source(tmp_path):
    """patients_matching.csv, as clinical_trials.csv names its columns."""
    with open(os.path.join(TEST_DIR, 'patients_matching.csv'), encoding='utf-8') as f:
        rows = f.read().replace('pid,disease_label', 'patient_id,clinical_trial_id')
    (tmp_path / 'clinical_trials.csv').write_text(rows, encoding='utf-8')
    return ('test/rml-fnml/reconciliation/clinical_trials.csv',
            str(tmp_path / 'clinical_trials.csv').replace('\\', '/'))


@pytest.mark.parametrize('matching', [None, 'EXACT', 'LOWERCASE', 'CASE-INSENSITIVE'])
# The second answer is read in memory, as the streaming parser rejects a
# solution, which matches nothing.
@pytest.mark.parametrize('path', ['/public/sparql', '/public/sparql/bad-term'])
def test_ways_of_matching_over_sparql(tmp_path, matching, path):
    """The entities a SPARQL query returns are matched in the same ways."""
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}{path}'),
            (STUDIES_QUERY, MATCHING_QUERY),
            patients_matching_source(tmp_path),
            *([with_sparql_input('morph-fn:matching', f'"{matching}"')] if matching else []),
        )
        g_morph = morph_kgc.materialize(sparql_config(mapping=mapping))

    assert {
        (str(s).replace('urn:id:', ''), str(o)) for s, o in g_morph.subject_objects()
    } == MATCHED_TERMS[matching or 'EXACT']


@pytest.mark.parametrize('matching, expected', [
    ('"lowercase"', 'LOWERCASE'),
    ('" Lowercase "', 'LOWERCASE'),
    ('"case-insensitive"', 'CASE-INSENSITIVE'),
])
def test_ways_of_matching_over_sparql_in_any_case(tmp_path, matching, expected):
    """The way of matching values over SPARQL is named in any case."""
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}/public/sparql'),
            (STUDIES_QUERY, MATCHING_QUERY),
            patients_matching_source(tmp_path),
            with_sparql_input('morph-fn:matching', matching),
        )
        g_morph = morph_kgc.materialize(sparql_config(mapping=mapping))

    assert {
        (str(s).replace('urn:id:', ''), str(o)) for s, o in g_morph.subject_objects()
    } == MATCHED_TERMS[expected]


def test_same_query_matched_two_ways_is_sent_once(tmp_path):
    """
    Executions sending the same query to the same endpoint, matching values in
    different ways, share a single answer, indexed for each.
    """
    with open(os.path.join(TEST_DIR, 'clinical_trials.csv'), encoding='utf-8') as f:
        rows = f.read().replace('NCT07667218', 'nct07667218')
    (tmp_path / 'clinical_trials.csv').write_text(rows, encoding='utf-8')
    source = str(tmp_path / 'clinical_trials.csv').replace('\\', '/')
    mapping = sparql_mapping(
        tmp_path,
        ('test/rml-fnml/reconciliation/clinical_trials.csv', source),
        with_sparql_execution_input(
            '<#ClinicalTrialReconciliationAgain>', 'morph-fn:matching', '"LOWERCASE"',
        ),
        mapping=SEVERAL_SPARQL_MAPPING,
    )
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(several_endpoints(server), mapping=mapping))

        assert server.requests.count(('/sparql', STUDIES_QUERY)) == 1

    # The lower-cased identifier of p1 only matches lower-cased.
    lowercased = ('urn:id:p1', 'https://data.com/id/1')
    assert objects_of(g_morph, 'participatesIn') == UNAMBIGUOUS_STUDIES
    assert objects_of(g_morph, 'enrolledIn') == UNAMBIGUOUS_STUDIES | {lowercased}


def test_executions_sending_the_same_query_share_one_answer(tmp_path, caplog):
    """
    Four executions sending the same query to the same endpoint, two matching
    values exactly and two case-insensitively, send it once, which is logged,
    and its answer is indexed for each way of matching. An execution sending
    another query sends it on its own.
    """
    with open(os.path.join(TEST_DIR, 'clinical_trials.csv'), encoding='utf-8') as f:
        rows = f.read().replace('NCT07667218', 'nct07667218')
    (tmp_path / 'clinical_trials.csv').write_text(rows, encoding='utf-8')
    executions = {
        'Exact1': ('EXACT', STUDIES_QUERY), 'Exact2': ('EXACT', STUDIES_QUERY),
        'CaseInsensitive1': ('CASE-INSENSITIVE', STUDIES_QUERY),
        'CaseInsensitive2': ('CASE-INSENSITIVE', STUDIES_QUERY),
        'Drugs': ('EXACT', DRUGS_QUERY),
    }
    mapping = tmp_path / 'mapping.ttl'
    mapping.write_text(
        '@prefix rml: <http://w3id.org/rml/> .\n'
        '@prefix grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#> .\n'
        '@prefix morph-fn: <urn:morph:function:> .\n'
        '@base <http://example.com/base/> .\n\n'
        '<#PatientsMapping> a rml:TriplesMap ;\n'
        '    rml:logicalSource [\n'
        f'        rml:source "{str(tmp_path / "clinical_trials.csv").replace(chr(92), "/")}" ;\n'
        '        rml:referenceFormulation rml:CSV\n'
        '    ] ;\n'
        + ''.join(
            f'    rml:predicateObjectMap [ rml:predicate <https://example.org/ontology/{name}> ; '
            f'rml:objectMap [ rml:functionExecution <#{name}> ; rml:termType rml:IRI ] ] ;\n'
            for name in executions
        )
        + '    rml:subjectMap [ rml:template "urn:id:{patient_id}" ] .\n\n'
        + ''.join(
            f'<#{name}> rml:function morph-fn:reconcileEntityOverSPARQL ;\n'
            '    rml:input [ rml:parameter morph-fn:sparqlEndpointUrl ; '
            f'rml:inputValue "{MAPPING_ENDPOINT}" ] ;\n'
            f'    rml:input [ rml:parameter morph-fn:sparqlQuery ; rml:inputValue """{query}""" ] ;\n'
            '    rml:input [ rml:parameter grel:valueParam ; rml:inputValueMap [ rml:reference '
            f'"{"drug" if name == "Drugs" else "clinical_trial_id"}" ] ] ;\n'
            f'    rml:input [ rml:parameter morph-fn:matching ; rml:inputValue "{matching}" ] .\n'
            for name, (matching, query) in executions.items()
        ),
        encoding='utf-8',
    )
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            endpoint_resource(f'{server.url}/sparql'), mapping=str(mapping),
        ))

        assert sorted(server.requests) == sorted([('/sparql', STUDIES_QUERY), ('/sparql', DRUGS_QUERY)])

    # The identifier of p1, lower-cased, is only matched case-insensitively.
    lowercased = ('urn:id:p1', 'https://data.com/id/1')
    for name in ('Exact1', 'Exact2'):
        assert objects_of(g_morph, name) == UNAMBIGUOUS_STUDIES
    for name in ('CaseInsensitive1', 'CaseInsensitive2'):
        assert objects_of(g_morph, name) == UNAMBIGUOUS_STUDIES | {lowercased}
    shared = [record.getMessage() for record in caplog.records if 'send the same query' in record.getMessage()]
    assert shared == [
        "Executions 'http://example.com/base/#CaseInsensitive1', "
        "'http://example.com/base/#CaseInsensitive2', 'http://example.com/base/#Exact1' and "
        "'http://example.com/base/#Exact2' of function "
        "'urn:morph:function:reconcileEntityOverSPARQL' send the same query to SPARQL "
        f"endpoint '{MAPPING_ENDPOINT}': it is sent once, and its answer is indexed for "
        "case-insensitive and exact matching."
    ]
    assert all(record.levelname == 'INFO' for record in caplog.records if 'send the same query' in record.getMessage())


@pytest.mark.parametrize('matching, expected', [
    ('EXACT', {('urn:id:p2', 'https://data.com/id/7')}),
    ('LOWERCASE', DIAGNOSES),
])
def test_ways_of_matching_several_values(tmp_path, matching, expected):
    """Each of the values an entity is matched by is matched the same way."""
    with open(os.path.join(TEST_DIR, 'diagnoses.csv'), encoding='utf-8') as f:
        rows = f.read().splitlines()
    # Lower-cased, but the row of p2.
    rows = [row if row.startswith('p2,') else row.lower() for row in rows]
    (tmp_path / 'diagnoses.csv').write_text('\n'.join(rows) + '\n', encoding='utf-8')
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(
                tmp_path, server,
                ('test/rml-fnml/reconciliation/diagnoses.csv',
                 str(tmp_path / 'diagnoses.csv').replace('\\', '/')),
                (SCHEME_INPUT, SCHEME_INPUT + execution_input('morph-fn:matching', f'"{matching}"')),
            ),
        ))

    assert diagnoses(g_morph) == expected


@pytest.mark.parametrize('binding, error', [
    ('rml:inputValue "FUZZY"',
     re.escape("binds 'urn:morph:function:matching' to 'FUZZY', which is not valid. "
               "Must be one of: CASE-INSENSITIVE, EXACT, LOWERCASE.")),
    ('rml:inputValueMap [ rml:reference "clinical_trial_id" ]',
     r"must bind 'urn:morph:function:matching' at most once, to a constant"),
])
def test_matching_over_sparql_not_a_valid_constant(tmp_path, binding, error):
    """A matching that is invalid, or read from the data, fails before the query is sent."""
    old, new = with_sparql_input('morph-fn:matching', '"LOWERCASE"')
    with VocabularyServer() as server:
        mapping = sparql_mapping(
            tmp_path,
            (MAPPING_ENDPOINT, f'{server.url}/public/sparql'),
            (old, new.replace('rml:inputValue "LOWERCASE"', binding)),
        )
        with pytest.raises(ValueError, match=error):
            morph_kgc.materialize(sparql_config(mapping=mapping))

        assert server.requests == []


# ── The same triples from a vocabulary file and a SPARQL endpoint ─────────────

PARITY_VOCABULARY = os.path.join(TEST_DIR, 'parity_vocabulary.ttl')
CLINICAL = 'https://example.org/vocabulary/clinical/'
# The query reading the labels of parity_vocabulary.ttl, the attributes the
# vocabulary file is matched against.
PARITY_QUERY = """
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT ?entity_iri ?matching_value_1 WHERE {
                ?entity_iri a skos:Concept ;
                            skos:prefLabel|skos:altLabel ?matching_value_1 .
            }
        """
# The concepts each row of parity_labels.csv is reconciled with, matching
# values in each way, with the ambiguity policy ALL. Rows 1 and 2 are those of
# the request: a trailing space, and 'A2' for 'A₂'.
PARITY_MATCHES = {
    'EXACT': {(4, 1)},
    'LOWERCASE': {(3, 2), (4, 1), (5, 3), (6, 4), (7, 5), (8, 1)},
    'CASE-INSENSITIVE': {
        (1, 1), (2, 2), (3, 2), (4, 1), (5, 3), (5, 4), (6, 3), (6, 4), (7, 5), (8, 1), (9, 1),
    },
}
# The ways an endpoint answers the query (see server.py).
PARITY_ENDPOINTS = [
    '/public/vocabulary/sparql',
    '/public/vocabulary/sparql/xml',
    '/public/vocabulary/sparql/no-head',
    '/public/vocabulary/sparql/results-first',
    '/public/vocabulary/sparql/gzip',
    '/public/vocabulary/sparql/bad-term',
]


def parity_config(tmp_path, function_inputs, matching, ambiguity, processes=1):
    """
    A configuration reconciling the labels of parity_labels.csv by an execution
    with *function_inputs*, matching values in the way *matching* and with the
    ambiguity policy *ambiguity*, otherwise the same for both functions.
    """
    # A file of its own, as a run reads it after the previous one ran.
    mapping = tmp_path / f'mapping_{len(list(tmp_path.glob("mapping_*.ttl")))}.ttl'
    mapping.write_text(
        '@prefix rml: <http://w3id.org/rml/> .\n'
        '@prefix grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#> .\n'
        '@prefix morph-fr: <urn:morph:function:reconciliation:> .\n'
        '@prefix morph-fn: <urn:morph:function:> .\n'
        '@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n'
        '@base <http://example.com/base/> .\n\n'
        '<#LabelsMapping>\n'
        '    a rml:TriplesMap ;\n'
        '    rml:logicalSource [\n'
        '        rml:source "test/rml-fnml/reconciliation/parity_labels.csv" ;\n'
        '        rml:referenceFormulation rml:CSV\n'
        '    ] ;\n'
        '    rml:subjectMap [ rml:template "urn:id:{id}" ] ;\n'
        '    rml:predicateObjectMap [\n'
        '        rml:predicate <https://example.org/ontology/matches> ;\n'
        '        rml:objectMap [ rml:functionExecution <#Reconciliation> ; rml:termType rml:IRI ]\n'
        '    ] .\n\n'
        '<#Reconciliation>\n'
        f'{function_inputs}'
        '    rml:input [ rml:parameter grel:valueParam ; rml:inputValueMap [ rml:reference "label" ] ] ;\n'
        f'    rml:input [ rml:parameter morph-fn:matching ; rml:inputValue "{matching}" ] ;\n'
        f'    rml:input [ rml:parameter morph-fn:ambiguity ; rml:inputValue "{ambiguity}" ] .\n',
        encoding='utf-8',
    )
    return (
        f'[CONFIGURATION]\n'
        f'number_of_processes={processes}\n'
        f'[RESOURCE:parity_vocabulary]\n'
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={PARITY_VOCABULARY}\n'
        f'[DataSource]\n'
        f'mappings={mapping}'
    )


def file_inputs():
    """The inputs of an execution reconciling against parity_vocabulary.ttl as a file."""
    return (
        '    rml:function morph-fr:reconcileVocabularyConcept ;\n'
        '    rml:input [ rml:parameter morph-fn:resource ; rml:inputValue "parity_vocabulary" ] ;\n'
        '    rml:input [ rml:parameter morph-fn:attributeIRI ; '
        'rml:inputValue skos:prefLabel , skos:altLabel ] ;\n'
    )


def sparql_inputs(endpoint):
    """The inputs of an execution reconciling against *endpoint*, holding parity_vocabulary.ttl."""
    return (
        '    rml:function morph-fn:reconcileEntityOverSPARQL ;\n'
        f'    rml:input [ rml:parameter morph-fn:sparqlEndpointUrl ; rml:inputValue "{endpoint}" ] ;\n'
        f'    rml:input [ rml:parameter morph-fn:sparqlQuery ; rml:inputValue """{PARITY_QUERY}""" ] ;\n'
    )


def parity_triples(matches):
    return {
        f'<urn:id:{row}> <https://example.org/ontology/matches> <{CLINICAL}{concept}>'
        for row, concept in matches
    }


@pytest.mark.parametrize('ambiguity', ['ALL', 'NONE'])
@pytest.mark.parametrize('matching', ['EXACT', 'LOWERCASE', 'CASE-INSENSITIVE'])
def test_same_triples_from_a_vocabulary_file_and_a_sparql_endpoint(tmp_path, matching, ambiguity):
    """
    The same data, mapping and way of matching values give the same triples
    whether the vocabulary is read from a file or queried from an endpoint
    holding it, whatever the shape of its answer: both are matched by Morph-KGC,
    without the query normalizing anything.
    """
    expected = PARITY_MATCHES[matching]
    if ambiguity == 'NONE':
        rows = [row for row, _ in expected]
        expected = {(row, concept) for row, concept in expected if rows.count(row) == 1}

    from_file = morph_kgc.materialize_set(parity_config(tmp_path, file_inputs(), matching, ambiguity))
    assert from_file == parity_triples(expected)

    with VocabularyServer() as server:
        for path in PARITY_ENDPOINTS:
            from_endpoint = morph_kgc.materialize_set(parity_config(
                tmp_path, sparql_inputs(f'{server.url}{path}'), matching, ambiguity,
            ))
            assert from_endpoint == from_file, path


def test_same_triples_from_a_vocabulary_file_and_a_sparql_endpoint_with_multiple_processes(tmp_path):
    """Worker processes match values alike, whichever the backend."""
    from_file = morph_kgc.materialize_set(
        parity_config(tmp_path, file_inputs(), 'CASE-INSENSITIVE', 'ALL', processes=4),
    )
    with VocabularyServer() as server:
        from_endpoint = morph_kgc.materialize_set(parity_config(
            tmp_path, sparql_inputs(f'{server.url}/public/vocabulary/sparql'),
            'CASE-INSENSITIVE', 'ALL', processes=4,
        ))

    assert from_endpoint == from_file == parity_triples(PARITY_MATCHES['CASE-INSENSITIVE'])


XML_ANSWER = '''<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
  <head><variable name="entity_iri"/><variable name="matching_value_1"/><variable name="g"/></head>
  <results>
    <result>
      <binding name="entity_iri"><uri> https://e/1 </uri></binding>
      <binding name="matching_value_1"><literal> Café\t</literal></binding>
    </result>
    <result>
      <binding name="entity_iri"><uri>https://e/2</uri></binding>
      <binding name="matching_value_1"><literal xml:lang="fr">   </literal></binding>
    </result>
    <result>
      <binding name="entity_iri"><uri>https://e/3</uri></binding>
      <binding name="matching_value_1"><literal datatype="http://www.w3.org/2001/XMLSchema#integer">7</literal></binding>
    </result>
    <result>
      <binding name="entity_iri"><uri>https://e/4</uri></binding>
      <binding name="matching_value_1"><bnode>b0</bnode></binding>
    </result>
    <result>
      <binding name="entity_iri"><uri>https://e/5</uri></binding>
      <binding name="matching_value_1"><literal></literal></binding>
    </result>
    <result>
      <binding name="entity_iri"><uri>https://e/6</uri></binding>
    </result>
  </results>
</sparql>
'''


def read_answer(tmp_path, body, content_type='application/sparql-results+xml'):
    from morph_kgc.http import Download
    from morph_kgc.reconciliation import sparql

    path = tmp_path / 'answer'
    path.write_bytes(body.encode('utf-8'))
    variables, solutions = sparql.read_solutions('E', Download(str(path), content_type))
    return variables, list(solutions)


def test_answer_in_xml_read_as_written(tmp_path):
    """
    The literals of an answer in XML are read exactly as written, whitespace
    included, as those of an answer in JSON are.
    """
    variables, solutions = read_answer(tmp_path, XML_ANSWER)

    assert variables == ['entity_iri', 'matching_value_1', 'g']
    assert solutions == [
        (('uri', 'https://e/1'), (('literal', ' Café\t'),)),
        (('uri', 'https://e/2'), (('literal', '   '),)),
        (('uri', 'https://e/3'), (('literal', '7'),)),
        (('uri', 'https://e/4'), (('bnode', 'b0'),)),
        (('uri', 'https://e/5'), (('literal', ''),)),
        (('uri', 'https://e/6'), (None,)),
    ]


def test_values_neither_iris_nor_literals_are_not_indexed(tmp_path):
    """
    A blank node, whose label only means something in the answer, is no value
    an entity is matched by, as it is not in a vocabulary file.
    """
    from morph_kgc.functions.tables import close_connections
    from morph_kgc.reconciliation import sparql

    indexes, _ = sparql._index_solutions(
        [
            (('uri', 'https://e/1'), (('bnode', 'b0'),)),
            (('uri', 'https://e/2'), (('literal', 'b0'),)),
            (('uri', 'https://e/3'), (('uri', 'b0'),)),
            (('uri', 'https://e/4'), (('triple', None),)),
        ],
        str(tmp_path), '512MB',
    )
    try:
        assert indexes['EXACT'].lookup('b0') == ['https://e/2', 'https://e/3']
    finally:
        close_connections(str(tmp_path))


def test_typed_literals_of_sparql_1_0_are_values(monkeypatch, tmp_path):
    """
    A literal an endpoint answers as a 'typed-literal', as in the JSON results
    of SPARQL 1.0 (Virtuoso), is a value, read in memory as well.
    """
    import json

    body = json.dumps({'results': {'bindings': [
        {'entity_iri': {'type': 'uri', 'value': 'https://e/1'},
         'matching_value_1': {'type': 'typed-literal', 'value': 'Enamel agenesis',
                              'datatype': 'http://www.w3.org/2001/XMLSchema#string'}},
        # A blank node as Virtuoso writes it, which the streaming parser rejects.
        {'entity_iri': {'type': 'bnode', 'value': 'nodeID://b10001'},
         'matching_value_1': {'type': 'literal', 'value': 'Unmatched'}},
    ]}})
    indexes = index_answer(monkeypatch, tmp_path, body, 'application/sparql-results+json')

    assert lookups(tmp_path, indexes['EXACT'], 'Enamel agenesis') == [['https://e/1']]


@pytest.mark.parametrize('body, error', [
    ('<?xml version="1.0"?><sparql xmlns="http://www.w3.org/2005/sparql-results#">'
     '<head/><boolean>true</boolean></sparql>', 'it is the answer to an ASK query'),
    ('<?xml version="1.0"?><sparql xmlns="http://www.w3.org/2005/sparql-results#">'
     '<results></results></sparql>', 'it has no head before its results'),
    ('<?xml version="1.0"?><sparql xmlns="http://www.w3.org/2005/sparql-results#"><head>',
     'did not answer with SPARQL results'),
    ('not XML at all', 'did not answer with SPARQL results'),
])
def test_answer_in_xml_that_is_not_select_results(tmp_path, body, error):
    """An answer in XML that holds no SELECT results fails, saying why."""
    with pytest.raises(ValueError, match=error):
        read_answer(tmp_path, body)


def index_answer(monkeypatch, tmp_path, body, content_type='application/sparql-results+xml',
                 download=None, **options):
    """The indexes sparql.build_index builds from the answer *body*, as if the endpoint answered it."""
    from morph_kgc.http import Download
    from morph_kgc.reconciliation import sparql

    path = tmp_path / 'answer'
    path.write_bytes(body.encode('utf-8') if isinstance(body, str) else body)
    answer = (download or Download)(str(path), content_type, temporary=True)
    monkeypatch.setattr(sparql, 'run_query', lambda resource, query, state_dir: answer)
    resource = ResourceConfig(name='E', resource_type='SPARQL_ENDPOINT', url='https://e/sparql')
    return sparql.build_index(
        resource, 'SELECT', state_dir=str(tmp_path), memory_limit='512MB', **options,
    )


def lookups(tmp_path, index, *values):
    from morph_kgc.functions.tables import close_connections
    try:
        return [index.lookup(value) for value in values]
    finally:
        close_connections(str(tmp_path))


@pytest.mark.parametrize('end', [
    '<binding name="entity_iri"><uri>https://e/3',
    '<uri>https://e/3',
])
def test_answer_in_xml_cut_short(monkeypatch, tmp_path, caplog, end):
    """
    An answer in XML cut short fails while its solutions are read, rather than
    being read in part by the lenient parser.
    """
    body = XML_ANSWER[:XML_ANSWER.index(end)]
    with pytest.raises(ValueError, match='did not answer with SPARQL results'):
        index_answer(monkeypatch, tmp_path, body)

    assert 'lenient parser' not in caplog.text


# Labels holding characters an answer in XML holds as they are written, but
# XML parsers cannot read as they are written.
LABELS_XML_CANNOT_READ = [
    'Agenesis\u000bof enamel', 'Two\r\nlines', 'Carriage\rreturn', ' padded\t',
    'a\x01b', 'x\ufffey', 'x\uffffy', 'form\x0cfeed',
    # The private-use characters standing in for them while they are read, and
    # the one escaping those, which a literal may hold as well.
    'x\U0010ff01y', 'x\U0010ff0dy', '\U0010ff00', 'x\U0010ff20y', 'x\U0010fffey', 'x\U0010ffffy',
]


def vocabulary_answer(writer, labels):
    """The answer in XML, written by *writer*, to a query returning *labels*."""
    import pyoxigraph
    from rdflib import Graph, Literal

    query = (
        'SELECT ?entity_iri ?matching_value_1 WHERE { '
        '?entity_iri <http://www.w3.org/2004/02/skos/core#prefLabel> ?matching_value_1 }'
    )
    if writer == 'rdflib':
        graph = Graph()
        for number, label in enumerate(labels):
            graph.add((URIRef(f'https://e/{number}'), URIRef(f'{SKOS}prefLabel'), Literal(label)))
        return graph.query(query).serialize(format='xml')
    store = pyoxigraph.Store()
    for number, label in enumerate(labels):
        store.add(pyoxigraph.Quad(
            pyoxigraph.NamedNode(f'https://e/{number}'),
            pyoxigraph.NamedNode(f'{SKOS}prefLabel'),
            pyoxigraph.Literal(label),
        ))
    return store.query(query).serialize(format=pyoxigraph.QueryResultsFormat.XML)


@pytest.mark.parametrize('writer', ['rdflib', 'oxigraph'])
def test_answer_in_xml_holding_characters_xml_cannot_read_as_written(tmp_path, writer):
    """
    Carriage returns, control characters, U+FFFE and U+FFFF, which stores
    write in XML as they are although XML parsers cannot read them so, are read
    as written, as from an answer in JSON: neither failing the answer nor
    turning a carriage return into a line feed.
    """
    path = tmp_path / 'answer'
    path.write_bytes(vocabulary_answer(writer, LABELS_XML_CANNOT_READ))
    from morph_kgc.http import Download
    from morph_kgc.reconciliation import sparql

    _, solutions = sparql.read_solutions('E', Download(str(path), 'application/sparql-results+xml'))

    assert sorted(value for _, ((_, value),) in solutions) == sorted(LABELS_XML_CANNOT_READ)


@pytest.mark.parametrize('chunk_size', [1, 2, 3, 5, 8, 13])
def test_answer_in_xml_read_in_any_chunks(tmp_path, monkeypatch, chunk_size):
    """
    The characters XML cannot read as written are read alike whatever the
    chunks the answer is read in, which may cut a tag, a reference or a
    character.
    """
    from morph_kgc.reconciliation import sparql

    body = vocabulary_answer('rdflib', LABELS_XML_CANNOT_READ).replace(
        b'a\x01b', b'a&#x1;b',
    ).replace(
        'x\U0010ff01y'.encode('utf-8'), b'x&#x10FF01;y',
    ).replace(
        'x\U0010ff20y'.encode('utf-8'), b'x&#1113888;y',
    ).replace(b'<literal>', b'<literal xml:lang="en">', 2)
    original = sparql._XmlLiteralFile.__init__

    def init(self, path, chunk_size_=chunk_size):
        original(self, path, chunk_size_)

    monkeypatch.setattr(sparql._XmlLiteralFile, '__init__', init)
    _, solutions = read_answer(tmp_path, body.decode('utf-8'))

    assert sorted(value for _, ((_, value),) in solutions) == sorted(LABELS_XML_CANNOT_READ)


@pytest.mark.parametrize('chunk_size', [1, 3, 7, 1 << 16])
@pytest.mark.parametrize('prefix', ['sr', 'r' * 200, 'é'])
def test_answer_in_xml_with_any_namespace_prefix(tmp_path, monkeypatch, chunk_size, prefix):
    """The literals of an answer in XML are read as written whatever their namespace prefix."""
    from morph_kgc.reconciliation import sparql

    original = sparql._XmlLiteralFile.__init__
    monkeypatch.setattr(
        sparql._XmlLiteralFile, '__init__', lambda self, path: original(self, path, chunk_size),
    )
    results = ''.join(
        f'<{prefix}:result><{prefix}:binding name="entity_iri"><{prefix}:uri>https://e/{number}'
        f'</{prefix}:uri></{prefix}:binding><{prefix}:binding name="matching_value_1">'
        f'<{prefix}:literal>a\r{number}&#x00000001;b</{prefix}:literal></{prefix}:binding></{prefix}:result>'
        for number in range(300)
    )
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?><{prefix}:sparql xmlns:{prefix}='
        f'"http://www.w3.org/2005/sparql-results#"><{prefix}:head><{prefix}:variable name="entity_iri"/>'
        f'<{prefix}:variable name="matching_value_1"/></{prefix}:head><{prefix}:results>{results}'
        f'</{prefix}:results></{prefix}:sparql>'
    )
    _, solutions = read_answer(tmp_path, body)

    assert [value for _, ((_, value),) in solutions] == [f'a\r{number}\x01b' for number in range(300)]


def test_answer_in_xml_in_another_encoding_read_as_it_is(tmp_path):
    """
    An answer declared in an encoding other than UTF-8 is read as an XML parser
    reads it, its carriage returns read as line feeds, rather than garbled.
    """
    from morph_kgc.http import Download
    from morph_kgc.reconciliation import sparql

    path = tmp_path / 'answer'
    path.write_bytes(XML_ANSWER.replace(
        '<?xml version="1.0"?>', '<?xml version="1.0" encoding="ISO-8859-1"?>',
    ).replace(' Café\t', 'Café\r\nline two').encode('latin-1'))
    _, solutions = sparql.read_solutions('E', Download(str(path), 'application/sparql-results+xml'))

    assert list(solutions)[0] == (('uri', 'https://e/1'), (('literal', 'Café\nline two'),))


@pytest.mark.parametrize('cut', ['head', 'results'])
def test_answer_in_xml_not_well_formed_read_by_a_lenient_parser(monkeypatch, tmp_path, caplog, cut):
    """
    An answer in XML the standard library rejects, in its head or partway
    through its results, is read by pyoxigraph, as it was before, saying that
    it trims the whitespace a literal starts or ends with.
    """
    from morph_kgc.reconciliation import sparql

    # Without the replacement of the characters XML cannot read as written.
    monkeypatch.setattr(sparql, '_XmlLiteralFile', lambda path: open(path, 'rb'))
    body = vocabulary_answer('rdflib', ['Agenesis\u000bof enamel', ' padded '])
    if cut == 'head':
        body = body.replace(b'<head>', b'<head><!-- \x0b -->')

    indexes = index_answer(monkeypatch, tmp_path, body)

    assert lookups(tmp_path, indexes['EXACT'], 'Agenesis\u000bof enamel', 'padded') == [
        ['https://e/0'], ['https://e/1'],
    ]
    assert 'is read by a lenient parser, which trims the whitespace' in caplog.text


def test_answer_closed_before_it_is_removed(monkeypatch, tmp_path):
    """
    An answer in XML read partway through, when indexing fails, is closed
    before it is removed, which Windows cannot do while it is open.
    """
    from morph_kgc.http import Download
    from morph_kgc.reconciliation import sparql

    opened = []
    reader = sparql._XmlLiteralFile

    class TrackedFile(reader):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            opened.append(self)

    class WindowsDownload(Download):
        def remove(self):
            if any(not answer_file.closed for answer_file in opened):
                raise PermissionError('The process cannot access the file: it is open.')
            super().remove()

    added = []
    add = sparql.ConceptIndexBuilder.add

    def add_until_the_disk_is_full(self, *args, **kwargs):
        added.append(args)
        if len(added) == 2:
            raise OSError('No space left on device')
        return add(self, *args, **kwargs)

    monkeypatch.setattr(sparql, '_XmlLiteralFile', TrackedFile)
    monkeypatch.setattr(sparql.ConceptIndexBuilder, 'add', add_until_the_disk_is_full)
    with pytest.raises(OSError, match='No space left on device'):
        index_answer(monkeypatch, tmp_path, XML_ANSWER, download=WindowsDownload)

    assert opened and not (tmp_path / 'answer').exists()


def test_answer_in_xml_read_with_the_same_memory(tmp_path):
    """An answer in XML is read as a stream: its solutions read are dropped."""
    import tracemalloc

    head, _, tail = XML_ANSWER.partition('<results>')
    results = ''.join(
        f'<result><binding name="entity_iri"><uri>https://e/{number}</uri></binding>'
        f'<binding name="matching_value_1"><literal>Label {number}</literal></binding></result>'
        for number in range(20000)
    )
    body = f'{head}<results>{results}</results></sparql>'
    from morph_kgc.http import Download
    from morph_kgc.reconciliation import sparql

    path = tmp_path / 'answer'
    path.write_text(body, encoding='utf-8')
    tracemalloc.start()
    try:
        _, solutions = sparql.read_solutions('E', Download(str(path), 'application/sparql-results+xml'))
        count = sum(1 for _ in solutions)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()

    assert count == 20000
    # Read whole, its 20,000 solutions take more than 20 MB.
    assert peak < 5 * 1024 * 1024


@pytest.mark.parametrize('matching', ['EXACT', 'LOWERCASE', 'CASE-INSENSITIVE'])
def test_same_triples_for_labels_xml_cannot_read_as_written(tmp_path, matching):
    """
    Labels holding control characters, carriage returns, U+FFFF or whitespace
    at their ends are matched alike from a vocabulary file and from an endpoint
    holding it, whatever the shape of its answer, XML included.
    """
    import csv
    import pandas as pd
    from rdflib import Literal

    labels = LABELS_XML_CANNOT_READ
    vocabulary = tmp_path / 'vocabulary.ttl'
    # Written as it is: rdflib writes a label holding a line feed as a long
    # literal holding it, which Windows would write as a carriage return and a
    # line feed, changing the label.
    vocabulary.write_text(
        '@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n' + ''.join(
            f'<{CLINICAL}{number}> a skos:Concept ; skos:prefLabel {Literal(label).n3()} .\n'
            for number, label in enumerate(labels, 1)
        ),
        encoding='utf-8',
        newline='\n',
    )
    # The labels, and the labels in upper case, as inputs.
    inputs = [*labels, *(label.upper() for label in labels)]
    with open(tmp_path / 'labels.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator='\n')
        writer.writerow(['id', 'label'])
        writer.writerows(enumerate(inputs, 1))
    assert list(pd.read_csv(tmp_path / 'labels.csv', dtype=str, keep_default_na=False)['label']) == inputs

    def config(function_inputs):
        return parity_config(tmp_path, function_inputs, matching, 'ALL').replace(
            f'url={PARITY_VOCABULARY}', f'url={vocabulary}',
        )

    def with_labels(text):
        mapping = re.search(r'mappings=(.*)', text).group(1)
        with open(mapping, encoding='utf-8') as f:
            rml = f.read()
        with open(mapping, 'w', encoding='utf-8') as f:
            f.write(rml.replace(
                'test/rml-fnml/reconciliation/parity_labels.csv',
                str(tmp_path / 'labels.csv').replace('\\', '/'),
            ))
        return text

    from_file = morph_kgc.materialize_set(with_labels(config(file_inputs())))
    # Each label matches itself, whatever the way of matching values.
    assert {
        f'<urn:id:{number}> <https://example.org/ontology/matches> <{CLINICAL}{number}>'
        for number in range(1, len(labels) + 1)
    } <= from_file

    with VocabularyServer(vocabulary=str(vocabulary)) as server:
        for path in PARITY_ENDPOINTS:
            from_endpoint = morph_kgc.materialize_set(
                with_labels(config(sparql_inputs(f'{server.url}{path}'))),
            )
            assert from_endpoint == from_file, path


# ── Reconciliation report ─────────────────────────────────────────────────────

DISEASE_EXECUTION = 'http://example.com/base/#DiseaseReconciliationExecution'
VOCABULARY_FUNCTION = 'urn:morph:function:reconciliation:reconcileVocabularyConcept'
SPARQL_FUNCTION = 'urn:morph:function:reconcileEntityOverSPARQL'


def read_report(path):
    import json

    with open(path, encoding='utf-8') as f:
        return [json.loads(line) for line in f]


def summaries(caplog):
    return [
        record.getMessage() for record in caplog.records
        if record.levelname == 'INFO' and record.getMessage().startswith('Execution ')
    ]


@pytest.mark.parametrize('processes', [1, 4])
def test_reconciliation_report(tmp_path, caplog, processes):
    """
    A value matched to no concept, and one matched to several, each occurring in
    several rows and reconciled for two triples maps, are reported once each,
    whatever the number of processes, and the execution is summarized.
    """
    with open(os.path.join(TEST_DIR, 'patients.csv'), encoding='utf-8') as f:
        rows = f.read()
    rows += f'pid_00004,Not a disease in the vocabulary\npid_00005,{AMBIGUOUS_LABEL}\n'
    (tmp_path / 'patients.csv').write_text(rows, encoding='utf-8')
    source = str(tmp_path / 'patients.csv').replace('\\', '/')
    mapping = mapping_copy(tmp_path, ('test/rml-fnml/reconciliation/patients.csv', source))
    report = tmp_path / 'reports' / 'report.jsonl'

    morph_kgc.materialize(ambiguous_config(
        mapping, 'ambiguity=NONE', processes, configuration=f'reconciliation_report={report}',
    ))

    shared = {
        'resource': 'disease_vocabulary',
        'matching': 'EXACT',
        'function_iri': VOCABULARY_FUNCTION,
        'execution_id': DISEASE_EXECUTION,
        'triples_maps': [
            'http://example.com/base/#DiseaseResourceMapping', 'http://example.com/base/#PatientsMapping',
        ],
        'sources': [source],
        'data_sources': ['DataSource'],
        'mappings': [mapping],
    }
    assert read_report(report) == [
        {
            'outcome': 'MULTIPLE_MATCH',
            'value': AMBIGUOUS_LABEL,
            'normalized_value': AMBIGUOUS_LABEL,
            'ambiguity': 'NONE',
            'candidates': [NARCOLEPSY, ADCA_DN],
            **shared,
        },
        {
            'outcome': 'NO_MATCH',
            'value': 'Not a disease in the vocabulary',
            'normalized_value': 'Not a disease in the vocabulary',
            **shared,
        },
    ]
    # The rule of PatientsMapping reconciles the 5 rows (1 matched, 2 unmatched,
    # 2 ambiguous), and each of the two rules of DiseaseResourceMapping, which
    # only reads the labels, the 3 distinct ones (1 of each).
    assert summaries(caplog) == [
        f"Execution '{DISEASE_EXECUTION}' of function '{VOCABULARY_FUNCTION}' handled 11 "
        "value(s): 3 matched, 4 unmatched (1 distinct), 4 ambiguous (1 distinct)."
    ]


def test_no_reconciliation_report_by_default(caplog):
    """Without 'reconciliation_report', no report is written, but the executions are summarized."""
    from morph_kgc.config.loaders import load_config

    assert load_config(ambiguous_config(MAPPING)).reconciliation_report == ''

    morph_kgc.materialize(ambiguous_config(MAPPING))

    assert not [record for record in caplog.records
                if record.getMessage().startswith('Reconciliation report written')]
    assert len(summaries(caplog)) == 1


def test_reconciliation_report_replaces_that_of_an_earlier_run(tmp_path):
    """The report of a run replaces that of an earlier run."""
    report = tmp_path / 'report.jsonl'
    report.write_text('a report of an earlier run\n', encoding='utf-8')

    morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\nurl={VOCABULARY}',
    ).replace('[CONFIGURATION]\n', f'[CONFIGURATION]\nreconciliation_report={report}\n'))

    # Pid_00003 matches no concept of the vocabulary.
    assert [record['value'] for record in read_report(report)] == ['Not a disease in the vocabulary']


@pytest.mark.parametrize('processes', [1, 4])
def test_reconciliation_report_of_a_run_aborted(tmp_path, processes):
    """A run aborted on an ambiguous value reports it."""
    report = tmp_path / 'report.jsonl'
    with pytest.raises(ValueError, match='MULTIPLE_MATCH'):
        morph_kgc.materialize(ambiguous_config(
            MAPPING, 'ambiguity=FAIL', processes, configuration=f'reconciliation_report={report}',
        ))

    assert {(record['outcome'], record['value'], record['ambiguity']) for record in read_report(report)
            if record['outcome'] == 'MULTIPLE_MATCH'} == {('MULTIPLE_MATCH', AMBIGUOUS_LABEL, 'FAIL')}


def test_reconciliation_report_over_sparql(tmp_path, caplog):
    """
    Values matched by several values together, against the answer to a query,
    are reported as arrays, with the endpoint and the hash of the query, as
    read and as looked up.
    """
    import hashlib

    # 'ICD 10' read as 'ICD-10' makes 'A|1' in 'ICD-10' match the codes of 6 and 7.
    query = CODES_QUERY.replace(
        'schema:codingSystem ?matching_value_2 .\n                }',
        'schema:codingSystem ?system .\n                }\n'
        '                BIND(IF(?system = "ICD 10", "ICD-10", ?system) AS ?matching_value_2)',
    )
    report = tmp_path / 'report.jsonl'
    with VocabularyServer() as server:
        morph_kgc.materialize(sparql_config(
            mapping=codes_mapping(
                tmp_path, server, (CODES_QUERY, query),
                (SCHEME_INPUT, SCHEME_INPUT + execution_input('morph-fn:matching', '"LOWERCASE"')),
            ),
        ).replace('[CONFIGURATION]\n', f'[CONFIGURATION]\nreconciliation_report={report}\n'))
        endpoint = f'{server.url}/public/sparql'

    shared = {
        'endpoint': endpoint,
        'query_hash': hashlib.sha256(query.encode('utf-8')).hexdigest(),
        'matching': 'LOWERCASE',
        'function_iri': SPARQL_FUNCTION,
        'triples_maps': ['http://example.com/base/#DiagnosesMapping'],
        'sources': ['test/rml-fnml/reconciliation/diagnoses.csv'],
        'data_sources': ['DataSource'],
    }
    records = read_report(report)
    for record in records:
        assert record.pop('execution_id') == 'http://example.com/base/#DiagnosisReconciliation'
        assert record.pop('mappings')
    assert records == [
        {
            'outcome': 'MULTIPLE_MATCH',
            'value': ['A|1', 'ICD-10'],
            'normalized_value': ['a|1', 'icd-10'],
            'ambiguity': 'ALL',
            'candidates': ['https://data.com/id/6', 'https://data.com/id/7'],
            **shared,
        },
        # 'A|1' in 'ICD 10', which the query reads as in 'ICD-10'.
        {
            'outcome': 'NO_MATCH',
            'value': ['A|1', 'ICD 10'],
            'normalized_value': ['a|1', 'icd 10'],
            **shared,
        },
        {
            'outcome': 'NO_MATCH',
            'value': ['A|1', 'ICD-11'],
            'normalized_value': ['a|1', 'icd-11'],
            **shared,
        },
    ]


def with_report(ini, report):
    """*ini* writing the reconciliation report to *report*."""
    return ini.replace('[CONFIGURATION]\n', f'[CONFIGURATION]\nreconciliation_report={report}\n')


NOT_A_DISEASE = 'Not a disease in the vocabulary'


@pytest.mark.parametrize('processes', [1, 4])
def test_reconciliation_report_one_record_per_execution(tmp_path, caplog, processes):
    """A value two executions match to no concept is reported once for each, and each is summarized."""
    report = tmp_path / 'report.jsonl'
    morph_kgc.materialize(with_report(config(
        f'resource_type=SKOS_VOCABULARY\nurl={VOCABULARY}',
        mapping=TWO_EXECUTIONS_MAPPING, processes=processes,
    ), report))

    records = [(record['execution_id'], record['outcome'], record['value']) for record in read_report(report)]
    assert len(records) == len(set(records))
    assert {(execution, value) for execution, _, value in records if value == NOT_A_DISEASE} == {
        ('http://example.com/base/#AltLabelReconciliationExecution', NOT_A_DISEASE),
        (DISEASE_EXECUTION, NOT_A_DISEASE),
    }
    assert len(summaries(caplog)) == 2


def test_reconciliation_report_one_record_per_resource(tmp_path):
    """A value matched to no concept of either of the resources its rows name is reported for each."""
    patients = tmp_path / 'patients.csv'
    patients.write_text(
        f'pid,disease_label,vocabulary\npid_00001,{NOT_A_DISEASE},vocabulary_a\n'
        f'pid_00002,{NOT_A_DISEASE},vocabulary_b\n',
        encoding='utf-8',
    )
    mapping = mapping_copy(
        tmp_path,
        ('test/rml-fnml/reconciliation/patients.csv', patients.as_posix()),
        ('rml:inputValue "disease_vocabulary"', 'rml:inputValueMap [ rml:reference "vocabulary" ]'),
    )
    report = tmp_path / 'report.jsonl'

    morph_kgc.materialize(two_vocabularies_config(
        mapping, url_a=VOCABULARY, configuration=f'reconciliation_report={report}',
    ))

    assert [(record['resource'], record['outcome'], record['value']) for record in read_report(report)] == [
        ('vocabulary_a', 'NO_MATCH', NOT_A_DISEASE), ('vocabulary_b', 'NO_MATCH', NOT_A_DISEASE),
    ]


@pytest.mark.parametrize('processes', [1, 4])
def test_reconciliation_report_gathers_the_candidates_of_every_row(tmp_path, processes):
    """
    A value matched against attributes read from the data, to several concepts
    in each row, is reported once, with the concepts of all of its rows.
    """
    vocabulary = tmp_path / 'vocabulary.ttl'
    vocabulary.write_text(
        '@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n'
        '<https://ex.org/A> a skos:Concept ; skos:prefLabel "X" .\n'
        '<https://ex.org/B> a skos:Concept ; skos:prefLabel "X" .\n'
        '<https://ex.org/C> a skos:Concept ; skos:prefLabel "Y" ; skos:altLabel "X" .\n'
        '<https://ex.org/D> a skos:Concept ; skos:prefLabel "Z" ; skos:altLabel "X" .\n',
        encoding='utf-8',
    )
    patients = tmp_path / 'patients.csv'
    patients.write_text(
        f'pid,disease_label,attribute\np1,X,{SKOS}prefLabel\np2,X,{SKOS}altLabel\n', encoding='utf-8',
    )
    mapping = mapping_copy(
        tmp_path,
        ('test/rml-fnml/reconciliation/patients.csv', patients.as_posix()),
        ('rml:inputValue skos:prefLabel , skos:altLabel', 'rml:inputValueMap [ rml:reference "attribute" ]'),
    )
    report = tmp_path / 'report.jsonl'

    g_morph = morph_kgc.materialize(with_report(config(
        f'resource_type=SKOS_VOCABULARY\nurl={vocabulary}', mapping=mapping, processes=processes,
    ), report))

    candidates = [f'https://ex.org/{concept}' for concept in 'ABCD']
    assert reconciled(g_morph) == {
        ('p1', candidates[0]), ('p1', candidates[1]), ('p2', candidates[2]), ('p2', candidates[3]),
    }
    assert [(record['outcome'], record['value'], record['candidates']) for record in read_report(report)] == [
        ('MULTIPLE_MATCH', 'X', candidates),
    ]


@pytest.mark.parametrize('report', [False, True])
def test_reconciliation_report_of_the_values_of_a_nested_function(tmp_path, report):
    """
    The values a nested function returns, of any type, are reconciled as
    strings, and reported as such: reporting them never fails the run.
    """
    udfs = tmp_path / 'udfs.py'
    udfs.write_text(
        'from decimal import Decimal\n\n'
        "@udf(fun_id='http://example.com/length', "
        "text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')\n"
        'def length(text):\n'
        '    return Decimal(len(text))\n',
        encoding='utf-8',
    )
    mapping = mapping_copy(
        tmp_path,
        ('rml:inputValueMap [ rml:reference "disease_label" ]',
         'rml:inputValueMap [ rml:functionExecution <#LengthExecution> ]'),
    )
    with open(mapping, 'a', encoding='utf-8', newline='\n') as f:
        f.write(
            '\n<#LengthExecution>\n'
            '    rml:function <http://example.com/length> ;\n'
            '    rml:input [\n'
            '        rml:parameter grel:valueParam ;\n'
            '        rml:inputValueMap [ rml:reference "disease_label" ]\n'
            '    ] .\n'
        )
    report_path = tmp_path / 'report.jsonl'
    ini = config(f'resource_type=SKOS_VOCABULARY\nurl={VOCABULARY}', mapping=mapping).replace(
        '[CONFIGURATION]\n', f'[CONFIGURATION]\nudfs={udfs}\n',
    )

    g_morph = morph_kgc.materialize(with_report(ini, report_path) if report else ini)

    assert not reconciled(g_morph)
    if report:
        with open(os.path.join(TEST_DIR, 'patients.csv'), encoding='utf-8') as f:
            labels = [line.rstrip('\n').split(',', 1)[1] for line in f.readlines()[1:]]
        # A record per distinct value: two of the labels are as long.
        assert sorted((record['outcome'], record['value'], record['normalized_value'])
                      for record in read_report(report_path)) == sorted(
            {('NO_MATCH', str(len(label)), str(len(label))) for label in labels}
        )


def test_reconciliation_report_of_a_list_a_nested_function_returns(tmp_path):
    """
    A list a nested function returns, reconciled against an index matching an
    entity by a single value, is looked up, and so reported, as a string.
    """
    patients = tmp_path / 'patients.csv'
    patients.write_text(
        f'pid,disease_label\npid_00001,{NOT_A_DISEASE}|{AMBIGUOUS_LABEL}\n', encoding='utf-8',
    )
    mapping = mapping_copy(
        tmp_path,
        ('test/rml-fnml/reconciliation/patients.csv', patients.as_posix()),
        ('rml:inputValueMap [ rml:reference "disease_label" ]',
         'rml:inputValueMap [ rml:functionExecution <#SplitExecution> ]'),
    )
    with open(mapping, 'a', encoding='utf-8', newline='\n') as f:
        f.write(
            '\n<#SplitExecution>\n'
            '    rml:function grel:string_split ;\n'
            '    rml:input [\n'
            '        rml:parameter grel:valueParameter ;\n'
            '        rml:inputValueMap [ rml:reference "disease_label" ]\n'
            '    ] ;\n'
            '    rml:input [ rml:parameter grel:p_string_sep ; rml:inputValue "|" ] .\n'
        )
    report = tmp_path / 'report.jsonl'

    morph_kgc.materialize(with_report(config(
        f'resource_type=SKOS_VOCABULARY\nurl={VOCABULARY}', mapping=mapping,
    ), report))

    # Looked up as the string of the list, not as two values.
    looked_up = str([NOT_A_DISEASE, AMBIGUOUS_LABEL])
    assert [(record['outcome'], record['value'], record['normalized_value'])
            for record in read_report(report)] == [('NO_MATCH', looked_up, looked_up)]


@pytest.mark.parametrize('reconciles', [True, False])
def test_reconciliation_report_emptied_when_nothing_is_reported(tmp_path, reconciles):
    """
    The report of a run reporting no value, matching every one or reconciling
    none, replaces that of an earlier run.
    """
    if reconciles:
        patients = tmp_path / 'patients.csv'
        with open(os.path.join(TEST_DIR, 'patients.csv'), encoding='utf-8') as f:
            # The rows of pid_00001 and pid_00002, which match a concept each.
            patients.write_text(''.join(f.readlines()[:3]), encoding='utf-8')
        mapping = mapping_copy(tmp_path, ('test/rml-fnml/reconciliation/patients.csv', patients.as_posix()))
    else:
        mapping = os.path.join(TEST_DIR, '..', '..', 'rml-core', 'csv', 'RMLTC0001a', 'mapping.ttl')
    report = tmp_path / 'report.jsonl'
    report.write_text('a report of an earlier run\n', encoding='utf-8')

    morph_kgc.materialize(with_report(config(
        f'resource_type=SKOS_VOCABULARY\nurl={VOCABULARY}', mapping=mapping,
    ), report))

    assert report.read_text(encoding='utf-8') == ''


def load_with_report(report):
    """The configuration of the vocabulary, writing the report to *report*, as it is read."""
    from morph_kgc.config.loaders import load_config

    return load_config(with_report(config(f'resource_type=SKOS_VOCABULARY\nurl={VOCABULARY}'), report))


@pytest.mark.parametrize('name', [
    'directory', 'reports' + os.sep, os.path.join('reports', 'sub', ''), os.path.join('reports', '.'),
])
def test_reconciliation_report_path_names_a_directory(tmp_path, name):
    """
    A report path naming a directory, existing or not, is an error as the
    configuration is read, and creates no directory.
    """
    (tmp_path / 'directory').mkdir()

    with pytest.raises(ValueError, match="'reconciliation_report' value .* names a directory"):
        load_with_report(f'{tmp_path}{os.sep}{name}')
    assert not (tmp_path / 'reports').exists()


def test_reconciliation_report_path_not_writable(tmp_path):
    """A report path whose directory cannot be created is an error as the configuration is read."""
    not_a_directory = tmp_path / 'file'
    not_a_directory.write_text('', encoding='utf-8')

    with pytest.raises(ValueError, match="'reconciliation_report' value .* cannot be created"):
        load_with_report(not_a_directory / 'report.jsonl')


@pytest.mark.parametrize('target', ['missing directory', 'loop'])
def test_reconciliation_report_path_of_a_link_not_writable(tmp_path, target):
    """A link the report cannot be written through is an error as the configuration is read."""
    report = tmp_path / 'report.jsonl'
    try:
        os.symlink(tmp_path / 'missing' / 'report.jsonl' if target == 'missing directory' else report, report)
    except (OSError, NotImplementedError):
        pytest.skip('links cannot be created')

    with pytest.raises(ValueError, match="'reconciliation_report' value .* is not a path the report can be written to"):
        load_with_report(report)


def test_reconciliation_report_path_created_with_its_directory_only(tmp_path):
    """Reading the configuration creates the directory of the report, but not the report."""
    report = tmp_path / 'reports' / 'report.jsonl'

    load_with_report(report)

    assert (tmp_path / 'reports').is_dir()
    assert not report.exists()


@pytest.mark.parametrize('denied, accepted', [('directory', True), ('file', False)])
def test_reconciliation_report_existing_file(tmp_path, monkeypatch, denied, accepted):
    """
    An existing report file, or a device such as /dev/stdout, needs only to be
    writable itself, whatever its directory.
    """
    report = tmp_path / 'reports' / 'report.jsonl'
    report.parent.mkdir()
    report.write_text('a report of an earlier run\n', encoding='utf-8')
    denied_path = os.fspath(report.parent if denied == 'directory' else report)
    access = os.access
    monkeypatch.setattr(os, 'access', lambda path, mode, *args, **kwargs: (
        False if os.fspath(path) == denied_path and mode & os.W_OK else access(path, mode, *args, **kwargs)
    ))

    if not accepted:
        with pytest.raises(ValueError, match="'reconciliation_report' value .* the file is not writable"):
            load_with_report(report)
        return
    loaded = load_with_report(report)
    monkeypatch.undo()

    morph_kgc.materialize(loaded)

    assert [record['value'] for record in read_report(report)] == [NOT_A_DISEASE]


@pytest.mark.skipif(
    sys.platform == 'win32' or os.geteuid() == 0,
    reason='the permissions of a directory are not enforced on Windows, nor for root',
)
def test_reconciliation_report_path_in_a_directory_not_writable(tmp_path):
    """A report to be created in a directory that cannot be written to is an error as the configuration is read."""
    read_only = tmp_path / 'read-only'
    read_only.mkdir()
    read_only.chmod(0o555)
    try:
        with pytest.raises(ValueError, match="'reconciliation_report' value .* is not a path the report can be written to"):
            load_with_report(read_only / 'report.jsonl')
    finally:
        read_only.chmod(0o755)


def test_reconciliation_report_leaves_out_a_line_not_written_whole(tmp_path, caplog):
    """
    A line of outcomes a process could not finish writing (a worker terminated
    as the run is aborted) is left out of the report, with a warning.
    """
    import json
    from types import SimpleNamespace

    from morph_kgc.functions.outcomes import report_outcomes

    state_dir = tmp_path / 'state'
    state_dir.mkdir()
    counts = {'counts': {'NO_MATCH': 1}, 'execution_id': '#E', 'function_iri': 'urn:f'}
    record = {
        'key': ['#E', 'NO_MATCH', 'v'],
        'rule': {'triples_map': '#TM', 'source': 'data.csv', 'data_source': 'DataSource'},
        'record': {'outcome': 'NO_MATCH', 'value': 'v', 'function_iri': 'urn:f', 'execution_id': '#E'},
    }
    (state_dir / 'outcomes-1.jsonl').write_text(
        f'{json.dumps(counts)}\n{json.dumps(record)}\n{json.dumps(record)[:30]}',
        encoding='ascii', newline='\n',
    )
    report = tmp_path / 'report.jsonl'

    report_outcomes(str(state_dir), SimpleNamespace(
        reconciliation_report=str(report),
        data_sources={'DataSource': SimpleNamespace(mappings=['mapping.ttl'])},
    ))

    assert read_report(report) == [{
        'outcome': 'NO_MATCH', 'value': 'v', 'function_iri': 'urn:f', 'execution_id': '#E',
        'triples_maps': ['#TM'], 'sources': ['data.csv'], 'data_sources': ['DataSource'],
        'mappings': ['mapping.ttl'],
    }]
    assert [record.getMessage() for record in caplog.records if record.levelname == 'WARNING'] == [
        f"Line 3 of '{state_dir / 'outcomes-1.jsonl'}' is not a whole outcome, it is left out."
    ]


def test_outcomes_argument_named_as_another(tmp_path):
    """A function cannot receive its outcomes under the name of another argument."""
    from morph_kgc.functions.bif_decorator import make_stateful_decorator

    with pytest.raises(ValueError, match="declares 'context' as its outcomes argument"):
        make_stateful_decorator({})(
            fun_id='http://example.com/f', initializer=lambda: {}, outcomes_parameter='context',
        )(lambda value, context: value)
