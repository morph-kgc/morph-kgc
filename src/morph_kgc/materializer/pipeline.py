from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Layout
------
Section A — Group-level materialization
    _triple_term_chains()           the base rules an rml:tripleTermMap resolves to
    _join_triple_term_chain()       joins the sources a chain of triple terms spans
    materialize_rule()              one rule  -> set[str] of N-Triples/N-Quads
    materialize_group_to_set()      one partition -> set[str]
    materialize_group_to_file()     one partition -> int  (triples written)

Section B — Top-level pipeline orchestration
    _parse_mappings()               parse + normalize RML mapping
    _asserted_groups()              filter non-asserted triples maps
    _collect()                      flatten list[set] -> set
    _triples_to_graph()             set -> rdflib.Graph | rdflib.Dataset
    _triples_to_oxigraph()          set -> pyoxigraph.Store
    materialize_pipeline()                  full pipeline, returns Graph | Store | set
"""

import logging
from io import BytesIO

import pandas as pd
from rdflib import Dataset, Graph

from ..constants import (
    LOGGING_NAMESPACE,
    NQUADS,
    RML_TRIPLES_MAP_CLASS,
    RML_TRIPLE_TERM_MAP,
)
from ..functions.state import initialize_contexts, release_contexts
from ..mapping.parser import MappingParser
from ..mapping.model import RMLMapping, RMLRule
from ..utils import prepare_output_files
from .executor import Executor, make_executor
from .stages.load import load_data
from .stages.references import collect_references, collect_parent_references_in_join_conditions
from .stages.terms import materialize_terms
from .stages.join import merge_data
from .stages.assemble import assemble_triples

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

# =============================================================================
# Section A — Group-level materialization
# =============================================================================

def _triple_term_chains(term_map, rml_mapping: RMLMapping) -> list[list[RMLRule]]:
    """
    Every chain of base rules an ``rml:tripleTermMap`` resolves to, one per
    triple term it generates.

    RML 1.2 has each predicate-object map of the base triples map contribute
    its own triple term, so a base triples map with several of them yields
    several chains. The first rule of a chain supplies the triple term itself
    and any further ones supply the nested triple terms of its object. A base
    triples map with no predicate-object map contributes no chain at all, which
    is how "a triple-term map pointing to a triples map with no
    predicate-object maps generates no triple term" falls out.
    """
    chains: list[list[RMLRule]] = []

    for base_rule in rml_mapping.get_rules(term_map.map_value):
        if base_rule.predicate is None or base_rule.object_ is None:
            continue
        nested_map = base_rule.object_
        if nested_map.map_type == RML_TRIPLE_TERM_MAP:
            chains.extend(
                [base_rule] + nested
                for nested in _triple_term_chains(nested_map, rml_mapping)
            )
        else:
            chains.append([base_rule])

    return chains


def materialize_rule(
    rule: RMLRule,
    rml_mapping: RMLMapping,
    config,
    python_source=None,
    nest_level: int = 0,
) -> set[str]:
    """
    Materialize one RML rule into a set of N-Triples / N-Quads strings.

    Parameters
    ----------
    rule:
        The RMLRule being processed.
    rml_mapping:
        Full parsed RMLMapping (needed for join resolution and FNML).
    config:
        Morph-KGC configuration object.
    python_source:
        Optional in-memory data for PYTHON_SOURCE logical sources.
    nest_level:
        Recursion depth for triple-term rules. At level 0 the graph
        component is appended for N-Quads output.

    Returns
    -------
    set[str]
        Each element is a complete, serialized N-Triple / N-Quad line
        (without a trailing newline).
    """
    object_map = rule.object_

    if object_map is not None and object_map.map_type == RML_TRIPLE_TERM_MAP:
        # one pass per triple term the base triples map contributes
        triples: set[str] = set()
        for chain in _triple_term_chains(object_map, rml_mapping):
            triples |= _materialize(
                rule, rml_mapping, config, python_source, nest_level, chain,
            )
        return triples

    return _materialize(rule, rml_mapping, config, python_source, nest_level, None)


def _join_triple_term_chain(
    data,
    rule: RMLRule,
    chain: list[RMLRule],
    rml_mapping: RMLMapping,
    config,
    python_source,
):
    """
    Bring in the logical source of every base rule in *chain* whose triple-term
    map joins against it, and report the column prefix each base rule's term
    maps must read.

    RML 1.2 evaluates the child maps of a join condition over the logical
    source of the triples map carrying the triple-term map, and the parent maps
    over the logical source of the base triples map. Down a chain of nested
    triple terms that child source is the level above, so each join merges onto
    the columns the previous one brought in. A level without a join condition
    stays on the data at hand, which is what "evaluated on the current logical
    iteration" means there.
    """
    # the term map that introduces each base rule: the rule's own object map
    # for the first, then the object map of the base rule above it
    term_maps = [rule.object_] + [base_rule.object_ for base_rule in chain[:-1]]

    aliases: list[str] = []
    current_alias = ""
    joins = 0

    for term_map, base_rule in zip(term_maps, chain):
        if term_map.join_conditions:
            joins += 1
            parent_alias = "parent_" if joins == 1 else f"parent{joins}_"

            parent_refs = collect_references(base_rule, rml_mapping)
            parent_refs.update(
                collect_parent_references_in_join_conditions(term_map.join_conditions)
            )
            parent_data = load_data(
                config, base_rule, parent_refs, python_source, rml_mapping,
            )
            data = merge_data(
                data, parent_data, term_map.join_conditions,
                parent_prefix=parent_alias, child_prefix=current_alias,
            )
            current_alias = parent_alias

        aliases.append(current_alias)

    return data, aliases


def _materialize(
    rule: RMLRule,
    rml_mapping: RMLMapping,
    config,
    python_source,
    nest_level: int,
    triple_term_chain: list[RMLRule] | None,
) -> set[str]:
    """Materialize *rule*, building one specific triple term when given a chain."""
    references = collect_references(rule, rml_mapping)

    if references:
        data = load_data(config, rule, references, python_source, rml_mapping)
        if data.empty:
            return set()
    else:
        # All term maps are constant-valued, so every iteration of the logical
        # source yields the same statements: a single row stands in for all of
        # them. The source is not read: its rows with no columns selected would
        # make a frame that pandas reports as empty, which generates nothing.
        data = pd.DataFrame(index=[0])

    om = rule.object_

    if triple_term_chain is not None:
        data, aliases = _join_triple_term_chain(
            data, rule, triple_term_chain, rml_mapping, config, python_source,
        )
        if data.empty:
            return set()
        data = materialize_terms(
            data, rule, rml_mapping, config,
            triple_term_chain=triple_term_chain, triple_term_aliases=aliases,
        )

    elif om is not None and om.join_conditions:
        # a referencing object map derives its term from the parent's subject alone
        parent_rule = rml_mapping.get_rule(om.map_value)
        parent_refs = collect_references(parent_rule, rml_mapping, only_subject_map=True)
        parent_refs.update(collect_parent_references_in_join_conditions(om.join_conditions))
        parent_data = load_data(config, parent_rule, parent_refs, python_source, rml_mapping)
        data = merge_data(data, parent_data, om.join_conditions)
        data = materialize_terms(data, rule, rml_mapping, config, columns_alias="parent_")

    else:
        data = materialize_terms(data, rule, rml_mapping, config)

    data = assemble_triples(data, rule, rml_mapping, config, nest_level=nest_level)

    return set(data["triple"].dropna().unique())


def materialize_group_to_set(
    group: list[RMLRule],
    rml_mapping: RMLMapping,
    config,
    python_source=None,
) -> set[str]:
    """Materialize one mapping partition into a set of N-Triple/N-Quad strings."""
    triples: set[str] = set()
    for rule in group:
        triples |= materialize_rule(rule, rml_mapping, config, python_source)
    return triples


def materialize_group_to_file(
    group: list[RMLRule],
    rml_mapping: RMLMapping,
    config,
    python_source=None,
) -> int:
    """
    Materialize one mapping partition and write results directly to the
    output file configured in *config*. Returns the number of triples written.
    """
    triples = materialize_group_to_set(group, rml_mapping, config, python_source)
    if not triples:
        return 0

    # Rules are partitioned by mapping_partition, so every rule in the group
    # writes to the same file. With no output_dir the name is ignored and all
    # groups append to the single configured output file.
    mapping_group = group[0].mapping_partition if group else None
    output_path = config.get_output_file_path(mapping_group)

    # The materialized triples carry no statement terminator, and in N-Quads
    # output they end with the graph term, which is empty for the default graph.
    with open(output_path, "a", encoding="utf-8") as fh:
        fh.write("".join(f"{triple.rstrip()} .\n" for triple in triples))

    return len(triples)


# =============================================================================
# Section B — Top-level pipeline orchestration
# =============================================================================

def _parse_mappings(config) -> RMLMapping:
    """Parse, normalize, and partition mapping rules."""
    parser = MappingParser(config)
    return parser.parse_mappings()


def _asserted_groups(rml_mapping: RMLMapping) -> list[list[RMLRule]]:
    """
    Return only the rule groups whose triples_map_type indicates asserted triples.

    In RML 1.2:
      - rml:TriplesMap               -> asserted (included)
      - rml:NonAssertedTriplesMap    -> triple-term only (excluded)

    Groups are already built by the partitioner; we filter at the group level
    so that non-asserted rules are available for triple-term resolution inside
    materialize_group_* but never trigger their own top-level materialisation.
    """
    asserted_types = {RML_TRIPLES_MAP_CLASS}
    groups: dict[str, list[RMLRule]] = {}

    for rule in rml_mapping.rules:
        if rule.triples_map_type in asserted_types:
            groups.setdefault(rule.mapping_partition, []).append(rule)

    return list(groups.values())


def _collect(results: list) -> set[str]:
    """Flatten a list of sets into one set."""
    out: set[str] = set()
    for r in results:
        if isinstance(r, set):
            out |= r
    return out


def _triples_to_graph(triples: set[str], config) -> Graph | Dataset:
    """
    Load the statements into an rdflib Graph, or into a Dataset for N-Quads
    output. N-Quads describe an RDF dataset, and parsing them into a plain
    Graph silently drops every statement that is not in the default graph.
    """
    graph = Dataset() if config.output_format == NQUADS else Graph()
    if triples:
        graph.parse(data=".\n".join(triples) + ".", format="nquads")
    return graph


def _triples_to_oxigraph(triples: set[str]):
    from pyoxigraph import RdfFormat, Store
    store = Store()
    if triples:
        store.bulk_load(
            BytesIO((".\n".join(triples) + ".").encode()),
            RdfFormat.N_QUADS,
        )
    return store


def materialize_pipeline(
    config,
    python_source=None,
    executor: Executor | None = None,
    output: str = "graph",
):
    """
    Full materialization pipeline.

    Parameters
    ----------
    config:
        Path, dict, or ConfigParser.
    python_source:
        Optional in-memory data structure for PYTHON_SOURCE logical sources.
    executor:
        Explicit Executor instance. When None, make_executor(config) decides
        between Sequential, Multiprocess, or Async based on config.
    output:
        One of ``"graph"`` | ``"oxigraph"`` | ``"set"`` | ``"file"``.

    Returns
    -------
    Graph     when output="graph" (default), Dataset for N-QUADS output
    Store     when output="oxigraph"
    set[str]  when output="set"
    int       when output="file"  (total triple count)
    """

    # ── 2. Mappings ───────────────────────────────────────────────────────
    LOGGER.info("Parsing and normalising mapping rules.")
    rml_mapping = _parse_mappings(config)

    # ── 3 & 4. Partition + filter asserted groups ─────────────────────────
    groups = _asserted_groups(rml_mapping)
    LOGGER.info(f"{len(groups)} mapping group(s) to materialize.")

    # ── 5. Executor ───────────────────────────────────────────────────────
    if executor is None:
        executor = make_executor(config)

    LOGGER.debug(f"Using executor: {type(executor).__name__}.")

    # ── 6. Shared context of stateful functions ───────────────────────────
    # Initialized before any triple is materialized, and persisted to disk so
    # that the worker processes read it back instead of rebuilding it.
    context_session = initialize_contexts(config, rml_mapping)

    try:
        # ── 7. Materialize + Serialize ────────────────────────────────────
        if output == "file":
            # Always wipe the output files first: the groups append to them.
            prepare_output_files(config, rml_mapping)
            results = executor.run(
                groups, materialize_group_to_file, rml_mapping, config
            )
            total = sum(r for r in results if isinstance(r, int))
            LOGGER.info(f"{total} triples written to file.")
            return total

        # "set", "graph", "oxigraph" — all collect into a set first
        results = executor.run(
            groups, materialize_group_to_set, rml_mapping, config,
            python_source=python_source,
        )
    finally:
        release_contexts(context_session)

    triples = _collect(results)
    LOGGER.info(f"{len(triples)} triples generated in total.")

    if output == "set":
        return triples
    if output == "oxigraph":
        return _triples_to_oxigraph(triples)
    return _triples_to_graph(triples, config)
