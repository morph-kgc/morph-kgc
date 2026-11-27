__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Stage 4 — Join
==============
Merges child and parent DataFrames on one or more join conditions.

Public API
----------
merge_data(child, parent, join_conditions) -> pd.DataFrame
"""

import pandas as pd

from ...mapping.model import JoinCondition
from .references import join_pairs, join_column
from ...constants import RML_REFERENCE, RML_CONSTANT
from ...utils import get_references_in_template


def _with_join_column(data: pd.DataFrame, value: str, value_type: str, prefix: str) -> pd.DataFrame:
    """*data* with the column join_column(value, value_type): the string values of a
    template (references read from the columns, with *prefix*) or of a constant."""
    column = prefix + join_column(value, value_type)
    if column in data.columns:
        return data
    data = data.copy()
    if value_type == RML_CONSTANT:
        data[column] = value
        return data
    template = value.replace("\\{", "{").replace("\\}", "}")
    values = pd.Series("", index=data.index)
    for ref in get_references_in_template(value):
        parts = template.split("{" + ref + "}", 1)
        values = values + parts[0] + data[prefix + ref].astype(str)
        template = parts[1] if len(parts) > 1 else ""
    data[column] = values + template
    return data


def merge_data(
    child: pd.DataFrame,
    parent: pd.DataFrame,
    join_conditions: list[JoinCondition],
    parent_prefix: str = "parent_",
    child_prefix: str = "",
) -> pd.DataFrame:
    """
    Inner-join *child* and *parent* DataFrames on *join_conditions*.
    Parent columns are prefixed with *parent_prefix* to avoid name collisions.
    Uses index-join when there is exactly one condition (faster path).

    *child_prefix* names the prefix the child side of the join already carries.
    It is empty for a join against the rule's own logical source, and set when
    joining onto columns an earlier join brought in, which is what a chain of
    nested triple terms does.
    """
    # a side given by a template or a constant (rml:childMap, rml:parentMap) is
    # joined on a column of its values
    for jc in join_conditions:
        if jc.child_type != RML_REFERENCE:
            child = _with_join_column(child, jc.child_value, jc.child_type, child_prefix)
        if jc.parent_type != RML_REFERENCE:
            parent = _with_join_column(parent, jc.parent_value, jc.parent_type, "")
    parent = parent.add_prefix(parent_prefix)
    child_refs, parent_refs = join_pairs(join_conditions)
    child_refs     = [child_prefix + r for r in child_refs]
    parent_refs_pf = [parent_prefix + r for r in parent_refs]

    if len(child_refs) == 1:
        child  = child.set_index(child_refs, drop=False)
        parent = parent.set_index(parent_refs_pf, drop=False)
        return child.join(parent, how="inner")

    return child.merge(parent, how="inner",
                       left_on=child_refs, right_on=parent_refs_pf)
