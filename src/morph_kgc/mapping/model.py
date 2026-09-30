"""
Typed dataclasses to represent parsed RML 1.2 rules.

Hierarchy
---------
RMLMapping
  └─ List[RMLRule]          one row per (triples-map × predicate-object-map) combination
        ├─ LogicalSource
        ├─ TermMap  (subject)
        ├─ TermMap  (predicate)   – None when the row carries no POM
        ├─ TermMap  (object)      – None when the row carries no POM
        ├─ TermMap  (graph)
        └─ List[JoinCondition]    – for rml:parentTriplesMap / rml:tripleTermMap joins

All string values that originate from RDF URIs are stored as plain strings
(the URI string) so that downstream code does not need rdflib as a dependency.
"""

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"


from dataclasses import dataclass, field
from typing import Optional
from ..functions.model import FNMLExecution


# ---------------------------------------------------------------------------
# Atomic building blocks
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class JoinCondition:
    """Represents a single rml:joinCondition (child/parent column pair)."""
    child_value: str
    parent_value: str


@dataclass(slots=True)
class TermMap:
    """
    A single RML term map (subject / predicate / object / graph).

    Attributes
    ----------
    map_type : str
        The RML property that expresses the value: one of
        ``rml:constant``, ``rml:template``, ``rml:reference``,
        ``rml:parentTriplesMap``, ``rml:tripleTermMap``,
        ``rml:functionExecution``.
    map_value : str
        The concrete value associated with *map_type* (IRI string, template
        string, column name, referenced triples-map IRI, …).
    term_type : Optional[str]
        The ``rml:termType`` URI (e.g. ``rml:IRI``, ``rml:Literal``,
        ``rml:BlankNode``, ``rml:TripleTerm``).
    join_conditions : list[JoinCondition]
        Join conditions attached to this term map (non-empty only for
        referencing / triple-term object maps that cross logical sources).

    Literal datatype, language & direction
    --------------------------------
    lang_datatype : Optional[str]
        Either ``rml:languageMap`` or ``rml:datatypeMap`` (the *property*
        used on the object map), or ``None``.
    lang_datatype_map_type : Optional[str]
        The map type for the language/datatype term map (``rml:constant``,
        ``rml:template``, ``rml:reference``).
    lang_datatype_map_value : Optional[str]
        The value for the language/datatype term map.
    direction_map_type : Optional[str]
        Map type for ``rml:directionMap`` (``rml:constant``, ``rml:template``,
        ``rml:reference``). ``None`` when no direction is specified.
    direction_map_value : Optional[str]
        Value for ``rml:directionMap`` – one of ``"ltr"`` or ``"rtl"`` when
        constant, or a column/template expression otherwise.
    """

    map_type: str
    map_value: str
    term_type: Optional[str] = None
    join_conditions: list[JoinCondition] = field(default_factory=list)

    # language / datatype
    lang_datatype: Optional[str] = None
    lang_datatype_map_type: Optional[str] = None
    lang_datatype_map_value: Optional[str] = None

    # RML 1.2 directional language-tagged strings
    direction_map_type: Optional[str] = None
    direction_map_value: Optional[str] = None


@dataclass(slots=True)
class LogicalSource:
    """Represents an rml:LogicalSource block."""
    format_: str     # RDB, CSV, JSON, XML, PARQUET, …
    value_type: str  # rml:query, rml:tableName, rml:source
    value: str       # file path, table name, SPARQL endpoint URL, …
    config_section_name: str        # config-file section name
    iterator: Optional[str] = None
    reference_formulation: Optional[str] = None


# ---------------------------------------------------------------------------
# Rule-level dataclass
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class RMLRule:
    """
    One normalized RML 1.2 rule.

    Attributes
    ----------
    triples_map_id : str
        IRI (or blank-node string) of the originating ``rml:TriplesMap``.
    triples_map_type : str
        One of ``rml:TriplesMap`` or ``rml:NonAssertedTriplesMap``.
    logical_source : LogicalSource
    subject : TermMap
    predicate : Optional[TermMap]
        ``None`` for triples maps that have no predicate-object map.
    object_ : Optional[TermMap]
        ``None`` when *predicate* is also ``None``.
    graph : Optional[TermMap]
        Defaults to ``rml:defaultGraph`` after normalization.
    mapping_partition : Optional[str]
        Assigned by ``MappingPartitioner``; ``None`` before partitioning.
    """

    triples_map_id: str
    triples_map_type: str
    logical_source: LogicalSource
    subject: TermMap
    predicate: Optional[TermMap] = None
    object_: Optional[TermMap] = None
    graph: Optional[TermMap] = None
    mapping_partition: Optional[str] = None


@dataclass(slots=True)
class HTTPAPIHeader:
    """A single header (field_name / field_value) for an HTTP API source."""
    field_name:  str
    field_value: str


@dataclass(slots=True)
class HTTPAPIEntry:
    """One HTTP-API source description."""
    source:        str
    absolute_path: str
    headers:       list[HTTPAPIHeader] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Top-level container
# ---------------------------------------------------------------------------

@dataclass
class RMLMapping:
    rules:            list[RMLRule]      = field(default_factory=list)
    fnml_executions:  dict[str, FNMLExecution] = field(default_factory=dict)
    http_api_entries: list[HTTPAPIEntry] = field(default_factory=list)

    def __len__(self):  return len(self.rules)
    def __iter__(self): return iter(self.rules)

    def get_rule(self, triples_map_id: str) -> RMLRule:
        """Look up a rule by its triples_map_id. Raises KeyError if not found."""
        for rule in self.rules:
            if rule.triples_map_id == triples_map_id:
                return rule
        raise KeyError(f"Triples map {triples_map_id!r} not found.")

    def get_rules(self, triples_map_id: str) -> list[RMLRule]:
        """
        Every rule of a triples map, one per predicate-object map.

        A triple-term map needs all of them: RML 1.2 has each predicate-object
        map of the base triples map contribute its own triple term.
        """
        rules = [rule for rule in self.rules if rule.triples_map_id == triples_map_id]
        if not rules:
            raise KeyError(f"Triples map {triples_map_id!r} not found.")
        return rules
