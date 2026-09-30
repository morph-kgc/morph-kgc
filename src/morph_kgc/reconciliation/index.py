from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Concept index
=============
The in-memory representation a reconciliation function is initialized with: for
each indexed attribute (``skos:prefLabel``, ``skos:altLabel``, ...) the values
it takes, and the concepts they identify.

The index is built once during the context initialization phase and is then
read-only, which is what makes it safe to share across mapping rules and worker
processes. It is a plain dataclass so that it can be pickled to the state
directory.
"""

import unicodedata
from dataclasses import dataclass, field

# Matching strategies, declared with the 'matching' option of a resource.
EXACT_MATCHING = "EXACT"
CASE_INSENSITIVE_MATCHING = "CASE-INSENSITIVE"
VALID_MATCHINGS = {EXACT_MATCHING, CASE_INSENSITIVE_MATCHING}


def normalize(value, matching: str) -> str:
    """Normalize a value to the key it is indexed and looked up under."""
    value = str(value)
    if matching == CASE_INSENSITIVE_MATCHING:
        # NFKC keeps compatibility characters from missing an obvious match.
        value = unicodedata.normalize("NFKC", value)
        value = " ".join(value.split()).casefold()
    return value


@dataclass
class ConceptIndex:
    """Values of a set of attributes, and the concepts they identify."""

    matching: str = EXACT_MATCHING
    # Attributes matched against when the mapping does not name any, in the
    # order they should be tried.
    default_attributes: tuple[str, ...] = ()
    # True when the index does not tell attributes apart, which is the case for
    # a SPARQL query that projects values without saying which property they
    # come from. Lookups then ignore the attributes the mapping declares.
    attribute_agnostic: bool = False
    # attribute IRI -> normalized value -> concept IRIs
    entries: dict[str, dict[str, list[str]]] = field(default_factory=dict)

    # -- Building -----------------------------------------------------------

    def add(self, attribute: str, value, concept: str) -> None:
        """Index *concept* under the value its *attribute* takes."""
        key = normalize(value, self.matching)
        concepts = self.entries.setdefault(attribute, {}).setdefault(key, [])
        if concept not in concepts:
            concepts.append(concept)

    # -- Looking up ---------------------------------------------------------

    def attributes(self) -> tuple[str, ...]:
        """Attributes to match against when the mapping does not name any."""
        return self.default_attributes or tuple(self.entries)

    def lookup(self, value, attributes=None) -> list[str]:
        """
        Concepts any of whose *attributes* takes *value*.

        A mapping matching against several attributes (``skos:prefLabel`` *or*
        ``skos:altLabel``) reconciles against their union: RDF puts no order on
        the values of a property, so no attribute can be given priority over
        another. The result is sorted, and therefore the same in every worker
        process.
        """
        if value is None:
            return []

        if self.attribute_agnostic:
            attributes = tuple(self.entries)
        elif not attributes:
            attributes = self.attributes()
        elif isinstance(attributes, str):
            attributes = (attributes,)

        key = normalize(value, self.matching)

        concepts = set()
        for attribute in attributes:
            concepts.update(self.entries.get(attribute, {}).get(key, ()))

        return sorted(concepts)

    # -- Reporting ----------------------------------------------------------

    def __len__(self) -> int:
        return sum(len(values) for values in self.entries.values())


@dataclass
class ReconciliationContext:
    """
    The shared context of a reconciliation function: one index per accessed
    resource, reachable through every name the mapping may use for it.
    """

    # resource name -> index
    indexes: dict[str, ConceptIndex] = field(default_factory=dict)
    # any other value a mapping may use for a resource (its URL) -> resource name
    aliases: dict[str, str] = field(default_factory=dict)

    def add(self, name: str, index: ConceptIndex) -> None:
        """Register the index built for the resource *name*."""
        self.indexes[name] = index

    def alias(self, name: str, *aliases: str) -> None:
        """Make the resource *name* reachable through other values as well."""
        for alias in aliases:
            if alias and alias != name:
                self.aliases[alias] = name

    def get(self, resource=None) -> ConceptIndex:
        """
        Return the index of *resource*, which may be named by its config
        section name or by its URL. When the mapping does not name a resource
        and exactly one is available, that one is used.
        """
        if resource is None or resource == "":
            if len(self.indexes) == 1:
                return next(iter(self.indexes.values()))
            raise ValueError(
                "The mapping does not say which resource to reconcile against, "
                f"and {len(self.indexes)} are available "
                f"({', '.join(sorted(self.indexes)) or 'none'}). Bind the "
                "'urn:morph:function:resource' parameter to a resource name."
            )

        resource = str(resource)
        name = self.aliases.get(resource, resource)

        if name not in self.indexes:
            raise ValueError(
                f"The mapping reconciles against the resource '{resource}', "
                "which was not initialized. Declare it in the configuration "
                f"file as '[RESOURCE:{resource}]'."
            )

        return self.indexes[name]
