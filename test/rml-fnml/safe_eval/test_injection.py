__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os

import pytest

import morph_kgc

from morph_kgc.functions.grel.safe_eval import UnsafeExpressionError


def test_data_value_cannot_execute_code_while_materializing(tmp_path):
    """A source value reaching grel:controls_if must not run as Python."""

    # the mapping builds the expression "'1' == '{monday}'", so this value
    # closes the quote and appends Python that writes a file if it is evaluated
    marker = tmp_path / "marker"
    injection = f"0' or __import__('pathlib').Path({str(marker)!r}).write_text('x') or '1"

    csv_path = tmp_path / "calendar.csv"
    csv_path.write_text(f'service_id,monday\n1,0\n2,"{injection}"\n')

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[DataSource]\nmappings:{mapping_path}\nfile_path:{csv_path}'

    with pytest.raises(UnsafeExpressionError):
        morph_kgc.materialize(config)

    assert not marker.exists()
