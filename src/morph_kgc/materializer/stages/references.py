__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Stage 2 — Reference Collection
===============================
Determines which source columns are needed to materialize a rule, including
join-condition columns and recursively nested FNML / triple-term maps.

Public API
----------
collect_references(rule, rml_mapping, only_subject_map=False) -> set[str]
join_pairs(join_conditions) -> (list[str], list[str])
"""
    
from ...constants import (
    RML_TEMPLATE, RML_REFERENCE, RML_EXECUTION,
    RML_TRIPLE_TERM_MAP, RML_PARENT_TRIPLES_MAP,
)
from ...functions.model import FNMLExecution
from ...mapping.model import RMLMapping, RMLRule, TermMap, JoinCondition
from ...utils import get_references_in_template


def join_pairs(join_conditions: list[JoinCondition]) -> tuple[list[str], list[str]]:
    """Unpack a list of JoinCondition into (child_refs, parent_refs)."""
    return (
        [jc.child_value for jc in join_conditions],
        [jc.parent_value for jc in join_conditions],
    )


def _refs_from_execution(execution: FNMLExecution) -> list[str]:
    """Source references used by *execution*, nested executions included."""
    refs = []
    for input_binding in execution.inputs:
        for value in input_binding.values:
            if value.map_type == RML_TEMPLATE:
                refs.extend(get_references_in_template(value.map_value))
            elif value.map_type == RML_REFERENCE:
                refs.append(value.map_value)
            elif value.map_type == RML_EXECUTION and value.nested_execution is not None:
                refs.extend(_refs_from_execution(value.nested_execution))
    return refs


def _refs_from_fnml(rml_mapping: RMLMapping, execution_id: str) -> list[str]:
    execution = rml_mapping.fnml_executions.get(execution_id)
    return _refs_from_execution(execution) if execution is not None else []


def _refs_from_annotation(
    map_type: str | None,
    map_value: str | None,
    rml_mapping: RMLMapping,
) -> list[str]:
    """Source references used by a language, datatype or direction term map."""
    if not map_value:
        return []
    if map_type == RML_TEMPLATE:
        return get_references_in_template(map_value)
    if map_type == RML_REFERENCE:
        return [map_value]
    if map_type == RML_EXECUTION:
        return _refs_from_fnml(rml_mapping, map_value)
    return []


def _refs_from_term_map(tm: TermMap | None, rml_mapping: RMLMapping) -> list[str]:
    if tm is None:
        return []
    if tm.map_type == RML_TEMPLATE:
        return get_references_in_template(tm.map_value)
    if tm.map_type == RML_REFERENCE:
        return [tm.map_value]
    if tm.map_type == RML_EXECUTION:
        return _refs_from_fnml(rml_mapping, tm.map_value)
    return []


def collect_references(
    rule: RMLRule,
    rml_mapping: RMLMapping,
    only_subject_map: bool = False,
) -> set[str]:
    """Return all source-column references needed to materialize *rule*."""
    refs: list[str] = []

    term_maps = (
        [rule.subject]
        if only_subject_map
        else [rule.subject, rule.predicate, rule.object_, rule.graph]
    )
    for tm in term_maps:
        refs.extend(_refs_from_term_map(tm, rml_mapping))

    if not only_subject_map and rule.object_ is not None:
        om = rule.object_
        refs.extend(_refs_from_annotation(
            om.lang_datatype_map_type, om.lang_datatype_map_value, rml_mapping,
        ))
        refs.extend(_refs_from_annotation(
            om.direction_map_type, om.direction_map_value, rml_mapping,
        ))

    if not only_subject_map and rule.object_ is not None:
        om = rule.object_

        if om.map_type == RML_TRIPLE_TERM_MAP:
            if om.join_conditions:
                # the base triples map is read from its own logical source, so
                # only the child side of the join comes from this rule's data
                child_refs, _ = join_pairs(om.join_conditions)
                refs.extend(child_refs)
            else:
                # same logical iteration: the base triples map's term maps are
                # evaluated on this rule's own data, so every reference of
                # every one of its rules must be present here
                for base_rule in rml_mapping.get_rules(om.map_value):
                    refs.extend(collect_references(base_rule, rml_mapping))

        elif om.map_type == RML_PARENT_TRIPLES_MAP:
            parent_rule = rml_mapping.get_rule(om.map_value)
            if om.join_conditions:
                # only need the join child-side columns here; parent-side
                # columns are collected separately when loading parent_data
                child_refs, _ = join_pairs(om.join_conditions)
                refs.extend(child_refs)
            else:
                # same logical source, row-aligned: parent's subject-map
                # references must be present in THIS rule's own data
                refs.extend(collect_references(parent_rule, rml_mapping, only_subject_map=True))

    return set(refs)


def collect_parent_references_in_join_conditions(
    join_conditions: list[JoinCondition],
) -> set[str]:
    """
    Return the set of parent references used in the given join conditions.

    For each JoinCondition(child_value=..., parent_value=...), this returns
    {parent_value}.
    """
    refs: set[str] = set()

    for jc in join_conditions:
        refs.add(jc.parent_value)

    return refs