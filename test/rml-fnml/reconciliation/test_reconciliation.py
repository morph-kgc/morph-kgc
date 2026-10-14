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
        "Must be one of: CASE-INSENSITIVE, EXACT."
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
    Values are matched exactly, so a mapping matching them whatever their case
    lowercases both the values of the query and the values it reconciles.
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
        r'projects \?matching_value_2',
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
