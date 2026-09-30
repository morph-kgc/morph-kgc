__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

import logging
import multiprocessing as mp

from copy import copy
from itertools import permutations

from ..constants import *
from .model import RMLMapping, RMLRule

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

# Sentinel used to force a non-match on the first iteration of every group.
_NO_MATCH = AUXILIAR_UNIQUE_REPLACING_STRING


##############################################################################
# Template invariant helper
##############################################################################

def get_invariant_of_template(template: str) -> str:
    """
    Return the part of a template before the first reference.
    Raises if the template contains no references (i.e. it is invalid).
    """
    escaped = template.replace('\\{', _NO_MATCH)
    if '{' not in escaped:
        raise Exception(
            f'Invalid template `{template}`. '
            f'No pairs of unescaped curly braces were found.'
        )
    invariant = escaped.split('{')[0]
    return invariant.replace(_NO_MATCH, '\\{')


##############################################################################
# Per-rule invariant helpers
##############################################################################

def _subject_invariant(rule: RMLRule) -> str:
    sm = rule.subject
    if sm.map_type == RML_TEMPLATE:
        return get_invariant_of_template(sm.map_value)
    if sm.map_type == RML_CONSTANT:
        return sm.map_value
    return ''


def _predicate_invariant(rule: RMLRule) -> str:
    if rule.predicate is None:
        return ''
    pm = rule.predicate
    if pm.map_type == RML_CONSTANT:
        return pm.map_value
    if pm.map_type == RML_TEMPLATE:
        return get_invariant_of_template(pm.map_value)
    return ''


def _object_invariant(rule: RMLRule, all_rules: list[RMLRule]) -> str:
    om = rule.object_
    if om is None:
        return ''
    if om.map_type == RML_CONSTANT:
        return om.map_value
    if om.map_type == RML_TEMPLATE:
        return get_invariant_of_template(om.map_value)
    if om.map_type == RML_PARENT_TRIPLES_MAP:
        # follow the join: use the subject invariant of the parent triples map
        parent = next(
            (r for r in all_rules if r.triples_map_id == om.map_value), None
        )
        if parent is not None:
            return _subject_invariant(parent)
    return ''


def _graph_invariant(rule: RMLRule) -> str:
    if rule.graph is None:
        return ''
    gm = rule.graph
    if gm.map_type == RML_CONSTANT:
        return gm.map_value
    if gm.map_type == RML_TEMPLATE:
        return get_invariant_of_template(gm.map_value)
    return ''


def _literal_type(rule: RMLRule) -> str:
    """
    Return the literal type discriminator used for object partitioning:
      - if lang_datatype_map_type is REFERENCE or TEMPLATE → use lang_datatype
      - otherwise → use lang_datatype_map_value
    """
    om = rule.object_
    if om is None:
        return ''
    if om.lang_datatype_map_type in (RML_REFERENCE, RML_TEMPLATE):
        return str(om.lang_datatype) if om.lang_datatype else ''
    return str(om.lang_datatype_map_value) if om.lang_datatype_map_value else ''


##############################################################################
# Internal data record
##############################################################################

class _PartitionRecord:
    """
    Lightweight record holding one rule plus derived partition fields.

    ``index`` is the position of the rule in the mapping. The maximal partition
    reorders records while it groups them, and runs each candidate ordering on
    copies (in a worker process, on copies of copies), so the index is what ties
    a computed partition label back to the rule it belongs to.
    """
    __slots__ = (
        'rule',
        'index',
        'subject_invariant',
        'predicate_invariant',
        'object_invariant',
        'graph_invariant',
        'literal_type',
        'mapping_partition',
    )

    def __init__(self, rule: RMLRule, all_rules: list[RMLRule], index: int):
        self.rule               = rule
        self.index              = index
        self.subject_invariant  = _subject_invariant(rule)
        self.predicate_invariant= _predicate_invariant(rule)
        self.object_invariant   = _object_invariant(rule, all_rules)
        self.graph_invariant    = _graph_invariant(rule)
        self.literal_type       = _literal_type(rule)
        self.mapping_partition  = ''


##############################################################################
# Maximal partition — position-ordering worker (module-level for mp.Pool)
##############################################################################

def _generate_maximal_partition_for_a_position_ordering(
    records: list[_PartitionRecord],
    position_ordering: tuple[str, ...],
) -> list[tuple[int, str]]:
    """
    Apply one S/P/O/G ordering to a copy of the records list and return the
    partition label of every rule as ``(rule index, label)``.  Module-level so
    multiprocessing can pickle it.

    Only the labels are returned: the records this works on are copies (of
    copies, in a worker process), so the objects themselves cannot be handed
    back to the caller — the indices are what reconnect the labels to the rules.
    """
    # Shallow copies: the ordering mutates `mapping_partition` and reorders the
    # list, but only ever reads through `rule`, which is shared with the caller.
    records = [copy(record) for record in records]

    for position in position_ordering:

        # ── SUBJECT ──────────────────────────────────────────────────────────
        if position == 'S':
            records.sort(key=lambda r: (r.mapping_partition, r.subject_invariant))
            current_global_group = records[0].mapping_partition if records else ''
            current_group   = 0
            current_invariant = _NO_MATCH

            for rec in records:
                if rec.mapping_partition != current_global_group:
                    current_group     = 0
                    current_invariant = _NO_MATCH
                    current_global_group = rec.mapping_partition

                sm = rec.rule.subject
                if sm.term_type == RML_BLANK_NODE:
                    rec.mapping_partition = f'{rec.mapping_partition}-0'
                elif rec.subject_invariant.startswith(current_invariant):
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'
                else:
                    current_group    += 1
                    current_invariant = rec.subject_invariant
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'

        # ── PREDICATE ────────────────────────────────────────────────────────
        elif position == 'P':
            records.sort(key=lambda r: (r.mapping_partition, r.predicate_invariant))
            all_constant_pred = all(
                r.rule.predicate is not None and r.rule.predicate.map_type == RML_CONSTANT
                for r in records
            )
            current_global_group = records[0].mapping_partition if records else ''
            current_group    = 0
            current_invariant = _NO_MATCH

            for rec in records:
                if rec.mapping_partition != current_global_group:
                    current_group     = 0
                    current_invariant = _NO_MATCH
                    current_global_group = rec.mapping_partition

                inv = rec.predicate_invariant
                if all_constant_pred and inv == current_invariant:
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'
                elif not all_constant_pred and inv.startswith(current_invariant):
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'
                else:
                    current_group    += 1
                    current_invariant = inv
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'

        # ── OBJECT ───────────────────────────────────────────────────────────
        elif position == 'O':
            records.sort(key=lambda r: (
                r.mapping_partition,
                str(r.rule.object_.term_type) if r.rule.object_ else '',
                r.literal_type,
                r.object_invariant,
            ))
            current_global_group = records[0].mapping_partition if records else ''
            current_group     = 0
            current_invariant  = _NO_MATCH
            current_lit_type   = _NO_MATCH

            for rec in records:
                if rec.mapping_partition != current_global_group:
                    current_group     = 0
                    current_invariant  = _NO_MATCH
                    current_lit_type   = _NO_MATCH
                    current_global_group = rec.mapping_partition

                om = rec.rule.object_
                ttype = om.term_type if om else None

                if ttype == RML_BLANK_NODE:
                    rec.mapping_partition = f'{rec.mapping_partition}-0'
                elif ttype == RML_LITERAL:
                    if rec.literal_type != current_lit_type:
                        current_group   += 1
                        current_lit_type = rec.literal_type
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'
                elif rec.object_invariant.startswith(current_invariant):
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'
                else:
                    current_group    += 1
                    current_invariant = rec.object_invariant
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'

        # ── GRAPH ────────────────────────────────────────────────────────────
        elif position == 'G':
            records.sort(key=lambda r: (r.mapping_partition, r.graph_invariant))
            all_constant_graph = all(
                r.rule.graph is not None and r.rule.graph.map_type == RML_CONSTANT
                for r in records
            )
            current_global_group = records[0].mapping_partition if records else ''
            current_group    = 0
            current_invariant = _NO_MATCH

            for rec in records:
                if rec.mapping_partition != current_global_group:
                    current_group     = 0
                    current_invariant = _NO_MATCH
                    current_global_group = rec.mapping_partition

                inv = rec.graph_invariant
                if all_constant_graph and inv == current_invariant:
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'
                elif not all_constant_graph and inv.startswith(current_invariant):
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'
                else:
                    current_group    += 1
                    current_invariant = inv
                    rec.mapping_partition = f'{rec.mapping_partition}-{current_group}'

    # Strip the leading '-' every position prepends to the label it builds.
    return [(record.index, record.mapping_partition.lstrip('-')) for record in records]


##############################################################################
# MappingPartitioner
##############################################################################

class MappingPartitioner:

    def __init__(self, rml_mapping: RMLMapping, config):
        self.rml_mapping = rml_mapping
        self.config      = config
        # Build records once; invariants are computed here, not on demand.
        self._records: list[_PartitionRecord] = [
            _PartitionRecord(rule, rml_mapping.rules, index)
            for index, rule in enumerate(rml_mapping.rules)
        ]

    def __len__(self):
        return len(self._records)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def partition_mappings(self) -> RMLMapping:
        """
        Assign mapping_partition to every RMLRule in self.rml_mapping.
        Returns the same RMLMapping object (mutated in place).
        """
        if self.config.is_partial_aggregations_partitioning():
            self._generate_partial_aggregations_partition()
        elif self.config.is_maximal_partitioning():
            self._generate_maximal_partition()
        elif self.config.is_no_partitioning():
            for rec in self._records:
                rec.mapping_partition = '0-0-0-0'
        else:
            LOGGER.error('Selected mapping partitioning algorithm is not valid.')

        # Write partitions back into the RMLRule objects.
        self._flush_partitions()

        unique_partitions = {rec.mapping_partition for rec in self._records}
        LOGGER.info(
            f'Mapping partition with {len(unique_partitions)} groups generated.'
        )
        counts = {}
        for rec in self._records:
            counts[rec.mapping_partition] = counts.get(rec.mapping_partition, 0) + 1
        LOGGER.info(
            f'Maximum number of rules within mapping group: {max(counts.values())}.'
        )

        return self.rml_mapping

    # ------------------------------------------------------------------
    # Partition strategies
    # ------------------------------------------------------------------

    def _generate_maximal_partition(self):
        position_orderings = list(permutations(['S', 'P', 'O', 'G']))

        if self.config.is_multiprocessing_enabled():
            with mp.Pool(self.config.number_of_processes) as pool:
                candidate_labels = pool.starmap(
                    _generate_maximal_partition_for_a_position_ordering,
                    zip(
                        [self._records] * len(position_orderings),
                        position_orderings,
                    ),
                )
        else:
            candidate_labels = [
                _generate_maximal_partition_for_a_position_ordering(
                    self._records, ordering
                )
                for ordering in position_orderings
            ]

        # The ordering that splits the mapping into the most groups wins.
        best_labels = max(
            candidate_labels,
            key=lambda labels: len({label for _, label in labels}),
        )

        for index, label in best_labels:
            self._records[index].mapping_partition = label

    def _generate_partial_aggregations_partition(self):
        """
        Independently partition by S, P, O, G and aggregate the four
        independent partition labels into a single 'S-P-O-G' label.
        """
        subject_parts   = self._partition_subjects()
        predicate_parts = self._partition_predicates()
        object_parts    = self._partition_objects()
        graph_parts     = self._partition_graphs()

        for rec, sp, pp, op, gp in zip(
            self._records,
            subject_parts,
            predicate_parts,
            object_parts,
            graph_parts,
        ):
            rec.mapping_partition = f'{sp}-{pp}-{op}-{gp}'

    # ------------------------------------------------------------------
    # Independent per-position partition helpers
    # ------------------------------------------------------------------

    def _partition_subjects(self) -> list[str]:
        order = sorted(
            range(len(self._records)),
            key=lambda i: self._records[i].subject_invariant,
        )
        result   = [''] * len(self._records)
        current_group    = 0
        current_invariant = _NO_MATCH

        for i in order:
            rec = self._records[i]
            sm  = rec.rule.subject
            if sm.term_type == RML_BLANK_NODE:
                result[i] = '0'
            elif rec.subject_invariant.startswith(current_invariant):
                result[i] = str(current_group)
            else:
                current_group    += 1
                current_invariant = rec.subject_invariant
                result[i] = str(current_group)
        return result

    def _partition_predicates(self) -> list[str]:
        all_constant = all(
            r.rule.predicate is not None and r.rule.predicate.map_type == RML_CONSTANT
            for r in self._records
        )
        order = sorted(
            range(len(self._records)),
            key=lambda i: self._records[i].predicate_invariant,
        )
        result    = [''] * len(self._records)
        current_group    = 0
        current_invariant = _NO_MATCH

        for i in order:
            rec = self._records[i]
            inv = rec.predicate_invariant
            if all_constant and inv == current_invariant:
                result[i] = str(current_group)
            elif not all_constant and inv.startswith(current_invariant):
                result[i] = str(current_group)
            else:
                current_group    += 1
                current_invariant = inv
                result[i] = str(current_group)
        return result

    def _partition_objects(self) -> list[str]:
        order = sorted(
            range(len(self._records)),
            key=lambda i: (
                str(self._records[i].rule.object_.term_type)
                if self._records[i].rule.object_ else '',
                self._records[i].literal_type,
                self._records[i].object_invariant,
            ),
        )
        result    = [''] * len(self._records)
        current_group    = 0
        current_invariant = _NO_MATCH
        current_lit_type  = _NO_MATCH

        for i in order:
            rec   = self._records[i]
            om    = rec.rule.object_
            ttype = om.term_type if om else None

            if ttype == RML_BLANK_NODE:
                result[i] = '0'
            elif ttype == RML_LITERAL:
                if rec.literal_type != current_lit_type:
                    current_group   += 1
                    current_lit_type = rec.literal_type
                result[i] = str(current_group)
            elif rec.object_invariant.startswith(current_invariant):
                result[i] = str(current_group)
            else:
                current_group    += 1
                current_invariant = rec.object_invariant
                result[i] = str(current_group)
        return result

    def _partition_graphs(self) -> list[str]:
        all_constant = all(
            r.rule.graph is not None and r.rule.graph.map_type == RML_CONSTANT
            for r in self._records
        )
        if all_constant:
            LOGGER.debug(
                'All graph maps are constant-valued, '
                'invariant subset is not enforced.'
            )
        order = sorted(
            range(len(self._records)),
            key=lambda i: self._records[i].graph_invariant,
        )
        result    = [''] * len(self._records)
        current_group    = 0
        current_invariant = _NO_MATCH

        for i in order:
            rec = self._records[i]
            inv = rec.graph_invariant
            if all_constant and inv == current_invariant:
                result[i] = str(current_group)
            elif not all_constant and inv.startswith(current_invariant):
                result[i] = str(current_group)
            else:
                current_group    += 1
                current_invariant = inv
                result[i] = str(current_group)
        return result

    # ------------------------------------------------------------------
    # Write-back
    # ------------------------------------------------------------------

    def _flush_partitions(self):
        """Copy computed mapping_partition from each record back to its RMLRule."""
        for rec in self._records:
            rec.rule.mapping_partition = rec.mapping_partition
