__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Shared helpers for the RML 1.2 test cases.

rdflib 7.x parses neither RDF 1.2 triple terms (``<<( s p o )>>``) nor
directional language-tagged strings (``"x"@en--ltr``), so these tests compare
N-Triples/N-Quads lines instead of comparing rdflib graphs the way the
RML-Core test cases do. Every mapping is written so that the generated terms
are deterministic (no generated blank node labels), which makes the line
comparison exact.
"""

import os
import subprocess
import sys

import pytest

import morph_kgc


@pytest.fixture
def rml12():
    """Expose the RML 1.2 helpers to every test case under ``test/rml12``."""
    return sys.modules[__name__]


def case_dir(test_file):
    """Directory of the test case that *test_file* belongs to."""
    return os.path.dirname(os.path.realpath(test_file))


def _config(mapping_path, output_format):
    return (
        f'[CONFIGURATION]\n'
        f'output_format={output_format}\n'
        f'number_of_processes=1\n'
        f'[DataSource]\n'
        f'mappings={mapping_path}'
    )


def materialize_lines(test_file, output_format='N-TRIPLES'):
    """Materialize the case's mapping and return its statements as a set."""
    mapping = os.path.join(case_dir(test_file), 'mapping.ttl')
    return {triple.strip() for triple in morph_kgc.materialize_set(_config(mapping, output_format))}


def expected_lines(test_file, expected_file='output.nt'):
    """Read the expected statements, dropping comments and the ' .' terminator."""
    path = os.path.join(case_dir(test_file), expected_file)
    with open(path, encoding='utf-8') as expected:
        lines = set()
        for raw_line in expected:
            line = raw_line.strip()
            if not line or line.startswith('#'):
                continue
            assert line.endswith('.'), f'{path}: statement does not end with a period: {line}'
            lines.add(line[:-1].strip())
        return lines


def assert_matches(test_file, expected_file='output.nt', output_format='N-TRIPLES'):
    """Assert the materialized statements are exactly the expected ones."""
    assert materialize_lines(test_file, output_format) == expected_lines(test_file, expected_file)


def assert_rejected(test_file, message_fragment):
    """Assert the mapping is rejected with an error mentioning *message_fragment*."""
    with pytest.raises(Exception) as error:
        materialize_lines(test_file)
    assert message_fragment.lower() in str(error.value).lower(), (
        f'expected an error mentioning {message_fragment!r}, got: {error.value}'
    )


def materialize_within(test_file, timeout_seconds=30):
    """
    Materialize in a subprocess so that a mapping the parser cannot normalize
    fails the test instead of hanging the suite.

    Returns the statements as a set; raises AssertionError on timeout and
    RuntimeError when materialization itself fails.
    """
    mapping = os.path.join(case_dir(test_file), 'mapping.ttl')
    script = (
        'import morph_kgc, sys\n'
        'for triple in morph_kgc.materialize_set(sys.argv[1]):\n'
        '    print(triple.strip())\n'
    )
    try:
        result = subprocess.run(
            [sys.executable, '-c', script, _config(mapping, 'N-TRIPLES')],
            capture_output=True, text=True, timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        raise AssertionError(
            f'materialization did not finish within {timeout_seconds} seconds'
        )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip().splitlines()[-1] if result.stderr else 'failed')

    return {line for line in result.stdout.splitlines() if line}
