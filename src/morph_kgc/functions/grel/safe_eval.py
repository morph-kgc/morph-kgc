from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Safe Evaluation of Data-Derived Expressions
===========================================
Some GREL functions receive expressions and array literals that are built from
the data being materialized (a CSV cell, a database value, a JSON field...).
Handing those to eval() would let any source value run arbitrary Python inside
the materialization process, so they are evaluated here against a restricted
AST that only accepts literals and operators: no names, no attribute access and
no calls, hence nothing to execute.

Public API
----------
safe_eval_expression(expression) -> object
"""

import ast
import operator


# An expression coming from the data is a comparison or a small literal, never
# a large tree. Bounding it keeps a crafted value from exhausting memory.
MAX_NODES = 1000
MAX_DEPTH = 25
# Guard against operators that turn a tiny expression into a huge value
# (e.g. "9 ** 9 ** 9" or "'a' * 10 ** 12").
MAX_INTEGER_BITS = 8192
MAX_SEQUENCE_LENGTH = 10 ** 6

_LITERAL_TYPES = (str, bytes, bool, int, float, complex, type(None), type(Ellipsis))
_SEQUENCE_TYPES = (str, bytes, list, tuple)

_UNARY_OPERATORS = {
    ast.Not:    operator.not_,
    ast.USub:   operator.neg,
    ast.UAdd:   operator.pos,
    ast.Invert: operator.invert,
}

_BINARY_OPERATORS = {
    ast.Add:      operator.add,
    ast.Sub:      operator.sub,
    ast.Mult:     operator.mul,
    ast.Div:      operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod:      operator.mod,
    ast.Pow:      operator.pow,
}

_COMPARISON_OPERATORS = {
    ast.Eq:    operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt:    operator.lt,
    ast.LtE:   operator.le,
    ast.Gt:    operator.gt,
    ast.GtE:   operator.ge,
    ast.Is:    operator.is_,
    ast.IsNot: operator.is_not,
    ast.In:    lambda element, container: element in container,
    ast.NotIn: lambda element, container: element not in container,
}


class UnsafeExpressionError(ValueError):
    """Raised when an expression uses something other than literals and operators."""


def safe_eval_expression(expression):
    """
    Evaluate *expression* allowing only literals, boolean, arithmetic and
    comparison operators. Anything else (names, calls, attribute access,
    subscripts, comprehensions...) raises UnsafeExpressionError.
    """

    if not isinstance(expression, str):
        raise UnsafeExpressionError(
            f"Expected an expression as a string, got {type(expression).__name__}."
        )

    try:
        tree = ast.parse(expression, mode="eval")
    except (SyntaxError, ValueError, MemoryError, RecursionError) as error:
        raise UnsafeExpressionError(f"Invalid expression {expression!r}: {error}.") from error

    node_count = 0
    for _ in ast.walk(tree):
        node_count += 1
        if node_count > MAX_NODES:
            raise UnsafeExpressionError(
                f"Expression {expression!r} is too large (over {MAX_NODES} nodes)."
            )

    return _evaluate(tree.body, expression, 0)


def _evaluate(node, expression, depth):
    if depth > MAX_DEPTH:
        raise UnsafeExpressionError(
            f"Expression {expression!r} is nested too deeply (over {MAX_DEPTH} levels)."
        )
    depth += 1

    if isinstance(node, ast.Constant):
        if not isinstance(node.value, _LITERAL_TYPES):
            raise UnsafeExpressionError(
                f"Constant of type {type(node.value).__name__} is not allowed "
                f"in expression {expression!r}."
            )
        return node.value

    elif isinstance(node, ast.BoolOp):
        # and/or short-circuit, so the operands are evaluated lazily
        if isinstance(node.op, ast.And):
            value = True
            for operand in node.values:
                value = _evaluate(operand, expression, depth)
                if not value:
                    return value
            return value
        elif isinstance(node.op, ast.Or):
            value = False
            for operand in node.values:
                value = _evaluate(operand, expression, depth)
                if value:
                    return value
            return value

    elif isinstance(node, ast.UnaryOp):
        unary_operator = _UNARY_OPERATORS.get(type(node.op))
        if unary_operator is not None:
            return unary_operator(_evaluate(node.operand, expression, depth))

    elif isinstance(node, ast.BinOp):
        binary_operator = _BINARY_OPERATORS.get(type(node.op))
        if binary_operator is not None:
            left = _evaluate(node.left, expression, depth)
            right = _evaluate(node.right, expression, depth)
            _check_result_size(node.op, left, right, expression)
            return binary_operator(left, right)

    elif isinstance(node, ast.Compare):
        left = _evaluate(node.left, expression, depth)
        for op, comparator_node in zip(node.ops, node.comparators):
            comparison_operator = _COMPARISON_OPERATORS.get(type(op))
            if comparison_operator is None:
                raise UnsafeExpressionError(
                    f"Comparison {type(op).__name__} is not allowed in expression {expression!r}."
                )
            right = _evaluate(comparator_node, expression, depth)
            if not comparison_operator(left, right):
                # comparisons are chained, so a false one short-circuits
                return False
            left = right
        return True

    elif isinstance(node, ast.IfExp):
        if _evaluate(node.test, expression, depth):
            return _evaluate(node.body, expression, depth)
        return _evaluate(node.orelse, expression, depth)

    elif isinstance(node, ast.List):
        return [_evaluate(element, expression, depth) for element in node.elts]

    elif isinstance(node, ast.Tuple):
        return tuple(_evaluate(element, expression, depth) for element in node.elts)

    elif isinstance(node, ast.Set):
        return {_evaluate(element, expression, depth) for element in node.elts}

    elif isinstance(node, ast.Dict):
        if None in node.keys:
            # dictionary unpacking, i.e. {**something}
            raise UnsafeExpressionError(
                f"Dictionary unpacking is not allowed in expression {expression!r}."
            )
        return {
            _evaluate(key, expression, depth): _evaluate(value, expression, depth)
            for key, value in zip(node.keys, node.values)
        }

    raise UnsafeExpressionError(
        f"{type(node).__name__} is not allowed in expression {expression!r}: only "
        f"literals and boolean, arithmetic and comparison operators can be used."
    )


def _check_result_size(op, left, right, expression):
    """Reject the operations whose result would blow up memory, before computing it."""

    if isinstance(op, ast.Mod) and isinstance(left, (str, bytes)):
        # printf-style formatting, where a single format specifier can ask for
        # an arbitrarily long result
        raise UnsafeExpressionError(
            f"String formatting with % is not allowed in expression {expression!r}."
        )

    # integers grow unbounded, and "10 ** 1000000" is as short to write as it is
    # expensive to compute
    if isinstance(left, int) and isinstance(right, int):
        if isinstance(op, ast.Pow):
            result_bits = left.bit_length() * abs(right)
        elif isinstance(op, ast.Mult):
            result_bits = left.bit_length() + right.bit_length()
        else:
            # every other operator shrinks its operands or grows them by a bit
            result_bits = 0

        if result_bits > MAX_INTEGER_BITS:
            raise UnsafeExpressionError(
                f"Integers over {MAX_INTEGER_BITS} bits cannot be computed "
                f"in expression {expression!r}."
            )

    # and so do strings, lists and tuples, through repetition and concatenation
    result_length = 0
    if isinstance(op, ast.Mult):
        for sequence, repetitions in ((left, right), (right, left)):
            if isinstance(sequence, _SEQUENCE_TYPES) and isinstance(repetitions, int):
                result_length = len(sequence) * repetitions
    elif isinstance(op, ast.Add):
        if isinstance(left, _SEQUENCE_TYPES) and isinstance(right, _SEQUENCE_TYPES):
            result_length = len(left) + len(right)

    if result_length > MAX_SEQUENCE_LENGTH:
        raise UnsafeExpressionError(
            f"Sequences longer than {MAX_SEQUENCE_LENGTH} cannot be built "
            f"in expression {expression!r}."
        )
