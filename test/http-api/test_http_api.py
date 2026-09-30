__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
HTTP API sources: a logical source whose `rml:source` is an HTTP request
described with the HTTP Vocabulary in RDF (`htv:`), read from the JSON the API
answers with.
"""

import os
import sys

import morph_kgc
import pytest

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from api_server import ApiServer, TOKEN   # noqa: E402

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
API_TOKEN_MODULE = os.path.join(TEST_DIR, 'api_token.py')

MAPPING_TEMPLATE = '''
@prefix rml:  <http://w3id.org/rml/> .
@prefix htv:  <http://www.w3.org/2011/http#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix ex:   <http://example.com/> .
@base <http://example.com/base/> .

<#PeopleAPI> a htv:Request ;
    htv:absoluteURI "{url}" ;
    htv:headers (
        [ htv:fieldName "Authorization" ; htv:fieldValue "{token}" ]
        {extra_headers}
    ) .

<#People> a rml:TriplesMap ;
    rml:logicalSource [
        rml:source <#PeopleAPI> ;
        rml:referenceFormulation rml:JSONPath ;
        rml:iterator "$.results[*]"
    ] ;
    rml:subjectMap [ rml:template "http://example.com/person/{{id}}" ] ;
    rml:predicateObjectMap [
        rml:predicate foaf:name ;
        rml:objectMap [ rml:reference "name" ]
    ] ;
    rml:predicateObjectMap [
        rml:predicate ex:department ;
        rml:objectMap [ rml:reference "department.name" ]
    ] .
'''


def mapping(tmp_path, url, token, extra_headers=''):
    path = tmp_path / 'mapping.ttl'
    path.write_text(MAPPING_TEMPLATE.format(
        url=url, token=token, extra_headers=extra_headers,
    ))
    return str(path)


def config(mapping_path, configuration_options=''):
    return (
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'{configuration_options}\n'
        f'[DataSource]\n'
        f'mappings={mapping_path}'
    )


def expected_graph():
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output.nq'))
    return g


def test_http_api_source(tmp_path):
    """The API is requested and its JSON response is materialized."""
    with ApiServer() as server:
        g_morph = morph_kgc.materialize(config(
            mapping(tmp_path, server.url, TOKEN)
        ))

        # The API is requested once per mapping group, so the number of
        # requests follows the partitioning rather than the mapping.
        assert {path for path, _, _ in server.requests} == {'/people'}

        _, headers, parameters = server.requests[0]
        assert headers['Authorization'] == TOKEN
        assert parameters == {}

    assert_isomorphic(expected_graph(), g_morph)


def test_http_api_header_value_from_environment_placeholder(tmp_path, monkeypatch):
    """A `{ENV_VAR}` placeholder in a header value is read from the environment."""
    monkeypatch.setenv('PEOPLE_API_SECRET', 's3cr3t')

    with ApiServer() as server:
        g_morph = morph_kgc.materialize(config(
            mapping(tmp_path, server.url, 'Bearer {PEOPLE_API_SECRET}'),
            'number_of_processes=1',
        ))

        _, headers, _ = server.requests[0]
        assert headers['Authorization'] == TOKEN

    assert_isomorphic(expected_graph(), g_morph)


def test_http_api_header_value_naming_an_environment_variable(tmp_path, monkeypatch):
    """A header value that is the bare name of an environment variable."""
    monkeypatch.setenv('PEOPLE_API_TOKEN', TOKEN)

    with ApiServer() as server:
        g_morph = morph_kgc.materialize(config(
            mapping(tmp_path, server.url, 'PEOPLE_API_TOKEN'),
            'number_of_processes=1',
        ))

        _, headers, _ = server.requests[0]
        assert headers['Authorization'] == TOKEN

    assert_isomorphic(expected_graph(), g_morph)


def test_http_api_token_from_api_token_module(tmp_path, monkeypatch):
    """The `api_token` module hands out the token the header value names."""
    monkeypatch.delenv('PEOPLE_API_TOKEN', raising=False)

    with ApiServer() as server:
        g_morph = morph_kgc.materialize(config(
            mapping(
                tmp_path, server.url, 'PEOPLE_API_TOKEN',
                extra_headers='[ htv:fieldName "Accept" ; htv:fieldValue "application/json" ]',
            ),
            f'number_of_processes=1\napi_token={API_TOKEN_MODULE}',
        ))

        _, headers, _ = server.requests[0]
        assert headers['Authorization'] == TOKEN
        # A value the module hands out no token for is sent as it is written.
        assert headers['Accept'] == 'application/json'

    assert_isomorphic(expected_graph(), g_morph)


def test_http_api_header_value_with_unset_environment_variable(tmp_path, monkeypatch):
    """A placeholder with nothing to replace it with is reported, not sent."""
    monkeypatch.delenv('PEOPLE_API_SECRET', raising=False)

    with ApiServer() as server:
        with pytest.raises(Exception, match='PEOPLE_API_SECRET'):
            morph_kgc.materialize(config(
                mapping(tmp_path, server.url, 'Bearer {PEOPLE_API_SECRET}'),
                'number_of_processes=1',
            ))

        assert server.requests == []


def test_http_api_field_that_is_not_a_header_is_a_query_parameter(tmp_path):
    """A field that is not one of the headers is sent in the query string."""
    with ApiServer() as server:
        g_morph = morph_kgc.materialize(config(
            mapping(
                tmp_path, server.url, TOKEN,
                extra_headers='[ htv:fieldName "format" ; htv:fieldValue "json" ]',
            ),
            'number_of_processes=1',
        ))

        _, headers, parameters = server.requests[0]
        assert parameters == {'format': 'json'}
        assert 'format' not in headers

    assert_isomorphic(expected_graph(), g_morph)


def test_http_api_rejected_request_is_reported(tmp_path):
    """The status an API answers a rejected request with reaches the user."""
    with ApiServer() as server:
        with pytest.raises(Exception, match='401'):
            morph_kgc.materialize(config(
                mapping(tmp_path, server.url, 'Bearer wrong-token'),
                'number_of_processes=1',
            ))
