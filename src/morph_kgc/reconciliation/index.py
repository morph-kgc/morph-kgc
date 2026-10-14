from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Concept index
=============
What a reconciliation function is initialized with: for each indexed attribute
(``skos:prefLabel``, ``skos:altLabel``, ...) the values it takes, and the
concepts they identify. The answer to a SPARQL query, which says nothing of
attributes, is indexed under a single, unnamed one.

The index is built once during the context initialization phase and is then
read-only, which is what makes it safe to share across mapping rules and worker
processes. Its entries are kept on disk, as a Parquet file of the state
directory of the run (see :mod:`morph_kgc.functions.tables`), sorted by the
normalized value they are looked up by: the shared context only holds the path
of the file, and a worker reads the parts of the file its lookups need, so that
memory does not grow with the size of the vocabulary.
"""

import unicodedata
from dataclasses import dataclass, field

import pandas as pd

from ..functions.tables import ParquetTable, TableBuilder, sql_string

# Matching strategies, which an execution chooses with the matching parameter.
EXACT_MATCHING = "EXACT"
CASE_INSENSITIVE_MATCHING = "CASE-INSENSITIVE"
VALID_MATCHINGS = {EXACT_MATCHING, CASE_INSENSITIVE_MATCHING}

# The columns of the file of an index.
INDEX_COLUMNS = {"key": "VARCHAR", "attribute": "VARCHAR", "concept": "VARCHAR"}

# The entries of the concepts that were not excluded.
INCLUDED = "WHERE concept NOT IN (SELECT concept FROM excluded)"


def parse_matching(value) -> str | None:
    """The matching strategy *value* names, whatever its case, or None."""
    matching = str(value).strip().upper()
    return matching if matching in VALID_MATCHINGS else None


def normalize(value, matching: str) -> str:
    """Normalize a value to the key it is indexed and looked up under."""
    value = str(value)
    if matching == CASE_INSENSITIVE_MATCHING:
        # NFKC keeps compatibility characters from missing an obvious match.
        value = unicodedata.normalize("NFKC", value)
        value = " ".join(value.split()).casefold()
    return value


@dataclass(frozen=True)
class ConceptIndex:
    """Values of a set of attributes, and the concepts they identify."""

    table: ParquetTable
    matching: str = EXACT_MATCHING
    # Attributes matched against when an execution does not name any, in the
    # order they should be tried; every indexed attribute when empty.
    default_attributes: tuple[str, ...] = ()
    # Every attribute the index holds values of.
    indexed_attributes: tuple[str, ...] = ()
    # The number of (value, attribute, concept) entries, and of concepts, for
    # reporting.
    size: int = 0
    concept_count: int = 0
    # The number of parts (row groups) of the file, which a lookup of a single
    # value reads one of.
    row_groups: int = 1

    # -- Looking up ---------------------------------------------------------

    def attributes(self) -> tuple[str, ...]:
        """Attributes to match against when an execution does not name any."""
        return self.default_attributes or self.indexed_attributes

    def lookup(self, value, attributes=None) -> list[str]:
        """Concepts any of whose *attributes* takes *value*."""
        return self.lookup_many([value], attributes)[0]

    def lookup_many(self, values, attributes=None) -> list[list[str]]:
        """
        For each of *values*, the concepts any of whose *attributes* takes it,
        looked up in a single query.

        A mapping matching against several attributes (``skos:prefLabel`` *or*
        ``skos:altLabel``) reconciles against their union: RDF puts no order on
        the values of a property, so no attribute can be given priority over
        another. Each result is sorted, and therefore the same in every worker
        process.
        """
        if not attributes:
            attributes = self.attributes()
        elif isinstance(attributes, str):
            attributes = (attributes,)
        attributes = [str(attribute) for attribute in attributes]

        keys = [None if value is None else normalize(value, self.matching) for value in values]
        distinct_keys = {key for key in keys if key is not None}

        concepts: dict[str, list[str]] = {}
        if distinct_keys and attributes:
            query = (
                "SELECT t.key, t.concept FROM {table} AS t "
                f"WHERE t.attribute IN ({', '.join('?' for _ in attributes)}) AND "
            )
            if len(distinct_keys) < self.row_groups:
                # A value is looked up in the one part of the file that may hold
                # it, while values spread over the file make it read every part:
                # a few values are cheaper to look up one by one.
                rows = [
                    row
                    for key in distinct_keys
                    for row in self.table.select(query + "t.key = ?", [*attributes, key])
                ]
            else:
                rows = self.table.select(
                    query + "t.key IN (SELECT key FROM lookup_keys)",
                    attributes,
                    lookup_keys=pd.DataFrame({"key": list(distinct_keys)}, dtype=object),
                )
            for key, concept in rows:
                concepts.setdefault(key, []).append(concept)

        return [
            sorted(set(concepts.get(key, ()))) if key is not None else []
            for key in keys
        ]

    # -- Reporting ----------------------------------------------------------

    def __len__(self) -> int:
        return self.size


class ConceptIndexBuilder:
    """
    Builds a :class:`ConceptIndex` for each of *matchings* (``EXACT`` when none
    is given), streaming their entries to disk as they are added, so that
    building the indexes of a vocabulary takes the same memory whatever its
    size. The entries of every matching go to a single scratch database, which
    keeps them within one memory limit.
    """

    def __init__(self, state_dir: str, memory_limit: str, *matchings: str) -> None:
        self.matchings = matchings or (EXACT_MATCHING,)
        self._builder = TableBuilder(state_dir, memory_limit)
        # matching -> scratch table of its entries
        self._entries = {
            matching: f"entries_{number}" for number, matching in enumerate(self.matchings)
        }
        for table in self._entries.values():
            self._builder.create(table, INDEX_COLUMNS)
        self._builder.create("excluded", {"concept": "VARCHAR"})
        self._attributes: dict[str, dict[str, None]] = {
            matching: {} for matching in self.matchings
        }

    def __enter__(self) -> "ConceptIndexBuilder":
        return self

    def __exit__(self, *exc_info) -> None:
        self._builder.close()

    def add(self, attribute: str, value, concept: str, matching: str | None = None) -> None:
        """
        Index *concept* under the value its *attribute* takes, for *matching*:
        the first of the builder when not given.
        """
        matching = matching or self.matchings[0]
        self._builder.append(
            self._entries[matching], (normalize(value, matching), attribute, concept),
        )
        self._attributes[matching][attribute] = None

    def exclude(self, concept: str) -> None:
        """Leave *concept* out of every index, whatever it was added under."""
        self._builder.append("excluded", (concept,))

    def build(
        self,
        default_attributes: tuple[str, ...] = (),
        matching: str | None = None,
    ) -> ConceptIndex:
        """
        Write the index of *matching*, the first of the builder when not given,
        to its file and return it.
        """
        matching = matching or self.matchings[0]
        table = self._builder.write(
            f"SELECT DISTINCT key, attribute, concept FROM {self._entries[matching]} "
            f"{INCLUDED} ORDER BY key, attribute, concept",
            prefix="index-",
        )
        # The number of entries and of row groups is read from the footer of
        # the file.
        path = sql_string(table.path)
        size, concept_count, row_groups = self._builder.fetch(
            f"SELECT (SELECT count(*) FROM read_parquet({path})), "
            f"(SELECT count(DISTINCT concept) FROM read_parquet({path})), "
            f"(SELECT count(DISTINCT row_group_id) FROM parquet_metadata({path}))"
        )[0]
        return ConceptIndex(
            table              = table,
            matching           = matching,
            default_attributes = tuple(default_attributes),
            indexed_attributes = tuple(self._attributes[matching]),
            size               = size,
            concept_count      = concept_count,
            row_groups         = max(row_groups, 1),
        )


@dataclass
class ReconciliationContext:
    """
    The shared context of a reconciliation function: the indexes of every
    accessed resource, one per way the mapping matches values against it,
    reachable through every name the mapping may use for the resource.
    """

    # resource name -> matching -> index
    indexes: dict[str, dict[str, ConceptIndex]] = field(default_factory=dict)
    # any other value a mapping may use for a resource (its URL) -> resource name
    aliases: dict[str, str] = field(default_factory=dict)

    def add(self, name: str, indexes: dict[str, ConceptIndex]) -> None:
        """Register the indexes built for the resource *name*, by matching."""
        self.indexes[name] = indexes

    def alias(self, name: str, *aliases: str, replace: bool = True) -> None:
        """
        Make the resource *name* reachable through other values as well. A
        value already reaching another resource keeps reaching it unless
        *replace*.
        """
        for alias in aliases:
            if alias and alias != name and (replace or alias not in self.aliases):
                self.aliases[alias] = name

    def get(self, resource=None, matching: str = EXACT_MATCHING) -> ConceptIndex:
        """
        Return the index of *resource* for *matching*. The resource may be named
        by its config section name or by its URL. When the mapping does not name
        a resource and exactly one is available, that one is used.
        """
        indexes = self._indexes(resource)
        if matching not in indexes:
            raise ValueError(
                f"The mapping reconciles against the resource '{resource}' with "
                f"{matching} matching, which it was not indexed for."
            )
        return indexes[matching]

    def _indexes(self, resource) -> dict[str, ConceptIndex]:
        """The indexes of *resource*, by matching."""
        if resource is None or resource == "":
            if len(self.indexes) == 1:
                return next(iter(self.indexes.values()))
            raise ValueError(
                "The mapping does not say which resource to reconcile against, "
                f"and {len(self.indexes)} are available "
                f"({', '.join(sorted(self.indexes)) or 'none'}). Bind the "
                "'urn:morph:function:resource' parameter to a resource name."
            )

        # A section name is never taken for the URL or the IRI of another
        # resource, as the configuration file resolves it.
        resource = str(resource)
        name = resource if resource in self.indexes else self.aliases.get(resource, resource)

        if name not in self.indexes:
            raise ValueError(
                f"The mapping reconciles against the resource '{resource}', "
                "which was not initialized. Declare it in the configuration "
                f"file as '[RESOURCE:{resource}]'."
            )

        return self.indexes[name]
