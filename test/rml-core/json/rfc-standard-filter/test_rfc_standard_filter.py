import morph_kgc

def test_rfc_standard_filter_crash():
    """
    This test attempts to use the '&&' operator in a JSONPath filter.
    On the current Morph-KGC, this should FAIL (raise an exception).
    """

    config = """
    [DataSource]
    mappings: test/rml-core/json/rfc-standard-filter/mapping.ttl
    """

    g = morph_kgc.materialize(config)

    # We expect 2 sensors (sens1 and sens4) to pass the filter.
    assert len(g) == 4  # 4 triples total (2 sensors * 2 triples each)