from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Concept index
=============
What a reconciliation function is initialized with: for each indexed attribute
(``skos:prefLabel``, ``skos:altLabel``, ...) the values it takes, and the
concepts they identify. The answer to a SPARQL query, which says nothing of
attributes, is indexed under a single, unnamed one, and may match entities by
several values together, such as a code and the scheme it belongs to: an index
then holds a column for each, and is looked up by the tuple of their values.

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

# Matching strategies, which an execution chooses with the matching parameter:
# - EXACT: values are compared as they are.
# - LOWERCASE: values are compared once lower-cased by str.lower(), and nothing
#   else.
# - CASE-INSENSITIVE: values are compared once normalized (NFKC), with their
#   whitespace collapsed, and case-folded (so that 'ß' matches 'ss').
EXACT_MATCHING = "EXACT"
LOWERCASE_MATCHING = "LOWERCASE"
CASE_INSENSITIVE_MATCHING = "CASE-INSENSITIVE"
VALID_MATCHINGS = {EXACT_MATCHING, LOWERCASE_MATCHING, CASE_INSENSITIVE_MATCHING}

# Policies for a value matching several concepts, which an execution chooses
# with the ambiguity parameter, and the 'ambiguity' option of a resource sets
# the default of: one term per concept, no term, or aborting the run.
AMBIGUITY_ALL = "ALL"
AMBIGUITY_NONE = "NONE"
AMBIGUITY_FAIL = "FAIL"
VALID_AMBIGUITIES = {AMBIGUITY_ALL, AMBIGUITY_NONE, AMBIGUITY_FAIL}
# The resource option setting the policy of the values reconciled against it.
AMBIGUITY_OPTION = "ambiguity"



def key_columns(arity: int = 1) -> tuple[str, ...]:
    """
    The columns of the file of an index holding the values an entry is looked
    up by, one per value of the *arity* it matches entities by.
    """
    return ("key", *(f"key_{position}" for position in range(2, arity + 1)))


def index_columns(arity: int = 1) -> dict[str, str]:
    """The columns of the file of an index matching entities by *arity* values."""
    return {
        **{column: "VARCHAR" for column in key_columns(arity)},
        "attribute": "VARCHAR",
        "concept": "VARCHAR",
    }


# The columns of the file of an index matching entities by a single value.
INDEX_COLUMNS = index_columns()

# The entries of the concepts that were not excluded.
INCLUDED = "WHERE concept NOT IN (SELECT concept FROM excluded)"


def parse_matching(value) -> str | None:
    """The matching strategy *value* names, whatever its case, or None."""
    matching = str(value).strip().upper()
    return matching if matching in VALID_MATCHINGS else None


def parse_ambiguity(value) -> str | None:
    """The policy for ambiguous values *value* names, whatever its case, or None."""
    ambiguity = str(value).strip().upper()
    return ambiguity if ambiguity in VALID_AMBIGUITIES else None


def normalize(value, matching: str) -> str:
    """Normalize a value to the key it is indexed and looked up under."""
    value = str(value)
    if matching == LOWERCASE_MATCHING:
        # Nothing but lower-casing: no whitespace trimmed or collapsed, and no
        # Unicode normalization.
        value = value.lower()
    elif matching == CASE_INSENSITIVE_MATCHING:
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
    # The number of values an entity is matched by, each looked up exactly, as
    # a whole: a value is a tuple of that many when more than one.
    arity: int = 1

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

        keys = [self._key(value) for value in values]
        distinct_keys = {key for key in keys if key is not None}

        concepts: dict[tuple[str, ...], list[str]] = {}
        if distinct_keys and attributes:
            columns = key_columns(self.arity)
            query = (
                f"SELECT {', '.join(f't.{column}' for column in columns)}, t.concept "
                "FROM {table} AS t "
                f"WHERE t.attribute IN ({', '.join('?' for _ in attributes)}) AND "
            )
            if len(distinct_keys) < self.row_groups:
                # A value is looked up in the one part of the file that may hold
                # it, while values spread over the file make it read every part:
                # a few values are cheaper to look up one by one.
                condition = " AND ".join(f"t.{column} = ?" for column in columns)
                rows = [
                    row
                    for key in distinct_keys
                    for row in self.table.select(query + condition, [*attributes, *key])
                ]
            else:
                # Values matched together are matched as a whole, column by
                # column: never joined into a string, which could make
                # different tuples look alike.
                condition = "t.key IN (SELECT key FROM lookup_keys)"
                if self.arity > 1:
                    condition += (
                        f" AND ({', '.join(f't.{column}' for column in columns)}) "
                        f"IN (SELECT {', '.join(columns)} FROM lookup_keys)"
                    )
                lookup_keys = pd.DataFrame(
                    {
                        column: [key[position] for key in distinct_keys]
                        for position, column in enumerate(columns)
                    },
                    dtype=object,
                )
                rows = self.table.select(query + condition, attributes, lookup_keys=lookup_keys)
            for *key, concept in rows:
                concepts.setdefault(tuple(key), []).append(concept)

        return [
            sorted(set(concepts.get(key, ()))) if key is not None else []
            for key in keys
        ]

    def _key(self, value) -> tuple[str, ...] | None:
        """
        The normalized values *value* is looked up by: a single value, or the
        tuple of the values an entity is matched by, when more than one. None
        when any of them is missing.
        """
        if self.arity == 1:
            components = (value,)
        elif isinstance(value, (tuple, list)) and len(value) == self.arity:
            components = tuple(value)
        else:
            raise ValueError(
                f"The index matches entities by {self.arity} values, so it is "
                f"looked up by tuples of {self.arity} values, not by {value!r}."
            )
        if any(component is None for component in components):
            return None
        return tuple(normalize(component, self.matching) for component in components)

    # -- Reporting ----------------------------------------------------------

    def __len__(self) -> int:
        return self.size


class ConceptIndexBuilder:
    """
    Builds a :class:`ConceptIndex` for each of *matchings* (``EXACT`` when none
    is given), streaming their entries to disk as they are added, so that
    building the indexes of a vocabulary takes the same memory whatever its
    size. The entries of every matching go to a single scratch database, which
    keeps them within one memory limit. The indexes match entities by *arity*
    values.
    """

    def __init__(
        self, state_dir: str, memory_limit: str, *matchings: str, arity: int = 1,
    ) -> None:
        self.matchings = matchings or (EXACT_MATCHING,)
        self.arity = arity
        self._builder = TableBuilder(state_dir, memory_limit)
        # matching -> scratch table of its entries
        self._entries = {
            matching: f"entries_{number}" for number, matching in enumerate(self.matchings)
        }
        for table in self._entries.values():
            self._builder.create(table, index_columns(arity))
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
        the first of the builder when not given. The value is the tuple of the
        values the concept is matched by, when more than one.
        """
        matching = matching or self.matchings[0]
        components = (value,) if self.arity == 1 else tuple(value)
        self._builder.append(
            self._entries[matching],
            (*(normalize(component, matching) for component in components), attribute, concept),
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
        keys = ", ".join(key_columns(self.arity))
        table = self._builder.write(
            f"SELECT DISTINCT {keys}, attribute, concept FROM {self._entries[matching]} "
            f"{INCLUDED} ORDER BY {keys}, attribute, concept",
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
            arity              = self.arity,
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
    # resource name -> the policy for the values matching several of its
    # concepts, when an execution does not choose one
    ambiguities: dict[str, str] = field(default_factory=dict)
    # The reports of ambiguous values this process has logged. The only part
    # of the context that is not read-only, so that each is logged once,
    # however many rows and mapping rules hold the value; it is written to
    # disk empty, and every process fills its own.
    reported: set = field(default_factory=set, compare=False, repr=False)

    def add(
        self,
        name: str,
        indexes: dict[str, ConceptIndex],
        ambiguity: str = AMBIGUITY_ALL,
    ) -> None:
        """
        Register the indexes built for the resource *name*, by matching, and its
        policy for ambiguous values.
        """
        self.indexes[name] = indexes
        self.ambiguities[name] = ambiguity

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
        indexes = self.indexes[self.name(resource)]
        if matching not in indexes:
            raise ValueError(
                f"The mapping reconciles against the resource '{resource}' with "
                f"{matching} matching, which it was not indexed for."
            )
        return indexes[matching]

    def ambiguity(self, resource=None) -> str:
        """The policy of *resource*, named as in :meth:`get`, for ambiguous values."""
        return self.ambiguities.get(self.name(resource), AMBIGUITY_ALL)

    def name(self, resource=None) -> str:
        """
        The name of the resource the mapping names *resource*: its section
        name, or any of its aliases, or nothing when only one is available.
        """
        if resource is None or resource == "":
            if len(self.indexes) == 1:
                return next(iter(self.indexes))
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

        return name


@dataclass
class QueryContext:
    """
    The shared context of the SPARQL reconciliation function: the indexes of the
    answer to every query of the mapping, by the endpoint it is sent to and the
    query, one per way the mapping matches values against it, and the policy
    of each endpoint for ambiguous values.
    """

    # (endpoint, query) -> matching -> index of its answer
    indexes: dict[tuple[str, str], dict[str, ConceptIndex]] = field(default_factory=dict)
    # endpoint -> the policy for the values matching several of the entities
    # its answers return, when an execution does not choose one
    ambiguities: dict[str, str] = field(default_factory=dict)
    # The reports of ambiguous values this process has logged, as in
    # ReconciliationContext.
    reported: set = field(default_factory=set, compare=False, repr=False)

    def ambiguity(self, endpoint: str) -> str:
        """The policy of *endpoint* for ambiguous values."""
        return self.ambiguities.get(endpoint, AMBIGUITY_ALL)
