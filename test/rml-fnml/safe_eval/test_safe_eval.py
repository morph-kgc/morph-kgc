__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import builtins

import pytest

from morph_kgc.functions.grel.array_functions import array_get, array_slice
from morph_kgc.functions.grel.control_functions import controls_if, controls_if_cast
from morph_kgc.functions.grel.safe_eval import UnsafeExpressionError, safe_eval_expression


SENTINEL = "_morph_kgc_safe_eval_sentinel"

# A data value that, if it were eval()ed, would run Python and still return a
# list, so that the function it is given to keeps working and the injection
# goes unnoticed.
INJECTION = f"__import__('builtins').{SENTINEL}.append('executed') or ['a', 'b']"


@pytest.fixture
def executed():
    """Collects the side effects of INJECTION, if it ever runs."""

    setattr(builtins, SENTINEL, [])
    yield getattr(builtins, SENTINEL)
    delattr(builtins, SENTINEL)


@pytest.mark.parametrize(
    "function",
    [
        lambda value: controls_if(value, "true", "false"),
        lambda value: controls_if_cast(value, "true", "false"),
        lambda value: array_get(value, "0"),
        lambda value: array_slice(value, "0"),
    ],
    ids=["controls_if", "controls_if_cast", "array_get", "array_slice"],
)
def test_data_values_cannot_execute_code(function, executed):
    # the function may reject the value or fall back to treating it as a plain
    # string, but it must never run it
    try:
        function(INJECTION)
    except UnsafeExpressionError:
        pass

    assert executed == []


@pytest.mark.parametrize(
    "expression",
    [
        "__import__('os').system('echo pwned')",
        "open('/etc/passwd').read()",
        "().__class__.__bases__",
        "[value for value in (1, 2)]",
        "(lambda: 1)()",
        "[1, 2, 3][0]",
        "monday",
        "f'{monday}'",
        "{**{'a': 1}}",
    ],
)
def test_unsafe_expressions_are_rejected(expression):
    with pytest.raises(UnsafeExpressionError):
        safe_eval_expression(expression)


@pytest.mark.parametrize(
    "expression",
    [
        "9 ** 9 ** 9",              # a huge integer, through exponentiation
        "(10 ** 2000) * (10 ** 2000)",  # a huge integer, through multiplication
        "'a' * 10 ** 12",           # a huge string, through repetition
        "'%.999999999f' % 1.0",     # a huge string, through printf formatting
        "1 + " * 30 + "1",          # a deeply nested expression
    ],
)
def test_resource_exhausting_expressions_are_rejected(expression):
    with pytest.raises(UnsafeExpressionError):
        safe_eval_expression(expression)


def test_invalid_expressions_are_rejected():
    with pytest.raises(UnsafeExpressionError):
        safe_eval_expression("'1' ==")
    with pytest.raises(UnsafeExpressionError):
        safe_eval_expression(1)


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("'1' == '1'", True),
        ("'1' == '0'", False),
        ("1 < 2 < 3", True),
        ("1 < 2 < 0", False),
        ("5 > 3 and 2 < 4", True),
        ("5 > 3 or 2 > 4", True),
        ("not 1 == 1", False),
        ("'a' in ['a', 'b']", True),
        ("(2 + 3) * 4 == 20", True),
        ("1 if 2 > 1 else 0", 1),
    ],
)
def test_expressions_are_evaluated(expression, expected):
    assert safe_eval_expression(expression) == expected


@pytest.mark.parametrize(
    "boolean_expression,expected",
    [
        ("true", "yes"),
        ("false", "no"),
        ("'1' == '1'", "yes"),
        ("'1' == '0'", "no"),
    ],
)
def test_controls_if_is_preserved(boolean_expression, expected):
    assert controls_if(boolean_expression, "yes", "no") == expected


@pytest.mark.parametrize(
    "string,expected",
    [
        ("", "no"),
        ("off", "no"),
        ("0", "no"),
        ("yes", "yes"),
        ("1", "yes"),
        ("2", "yes"),
    ],
)
def test_controls_if_cast_is_preserved(string, expected):
    assert controls_if_cast(string, "yes", "no") == expected


def test_array_functions_are_preserved():
    assert array_get("['Alice', 'Bob', 'Charlie']", "0") == "Alice"
    assert array_get("['Alice', 'Bob', 'Charlie']", "0", "2") == ["Alice", "Bob"]
    assert array_get(["Toyota", "Corolla"], "1") == "Corolla"
    # a value that is not a list is handled as a string, as before
    assert array_get("Toyota", "0") == "T"

    assert array_slice("['Alice', 'Bob', 'Charlie']", "1") == str(["Bob", "Charlie"])
    assert array_slice("['Alice', 'Bob', 'Charlie']", "0", "2") == str(["Alice", "Bob"])
    assert array_slice("Toyota", "2") == "yota"
