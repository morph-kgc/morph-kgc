__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML-Core namespace constants (http://w3id.org/rml/).

Covers RML-Core and RML 1.2 extension.
"""

RML_NAMESPACE = "http://w3id.org/rml/"

# ── Core classes ──────────────────────────────────────────────────────────────
RML_LOGICAL_SOURCE_CLASS = f"{RML_NAMESPACE}LogicalSource"
RML_TRIPLES_MAP_CLASS = f"{RML_NAMESPACE}TriplesMap"
RML_TERM_MAP_CLASS = f"{RML_NAMESPACE}TermMap"
RML_SUBJECT_MAP_CLASS = f"{RML_NAMESPACE}SubjectMap"
RML_PREDICATE_OBJECT_MAP_CLASS = f"{RML_NAMESPACE}PredicateObjectMap"
RML_PREDICATE_MAP_CLASS = f"{RML_NAMESPACE}PredicateMap"
RML_OBJECT_MAP_CLASS = f"{RML_NAMESPACE}ObjectMap"
RML_REF_OBJECT_MAP_CLASS = f"{RML_NAMESPACE}RefObjectMap"
RML_GRAPH_MAP_CLASS = f"{RML_NAMESPACE}GraphMap"
RML_JOIN_CLASS = f"{RML_NAMESPACE}Join"

# ── RML 1.2 triples-map subclasses ───────────────────────────────────────────
RML_NON_ASSERTED_TRIPLES_MAP_CLASS = f"{RML_NAMESPACE}NonAssertedTriplesMap"

# ── RML 1.2 triple-term map ───────────────────────────────────────────────────
RML_TRIPLE_TERM_MAP_CLASS    = f"{RML_NAMESPACE}TripleTermMap"   # rdf:type on object map node
RML_TRIPLE_TERM_MAP          = f"{RML_NAMESPACE}tripleTermMap"   # predicate
RML_TRIPLE_TERM              = f"{RML_NAMESPACE}TripleTerm"      # rml:termType value

# ── RML 1.2 direction map ─────────────────────────────────────────────────────
RML_DIRECTION_MAP      = f"{RML_NAMESPACE}directionMap"
RML_DIRECTION_SHORTCUT = f"{RML_NAMESPACE}direction"

# The only base directions RDF 1.2 defines.
RML_DIRECTION_LTR = "ltr"
RML_DIRECTION_RTL = "rtl"
VALID_DIRECTIONS  = (RML_DIRECTION_LTR, RML_DIRECTION_RTL)

# ── RML 1.2 reifyingMap shortcut ─────────────────────────────────────────────
RML_REIFYING_MAP = f"{RML_NAMESPACE}reifyingMap"

# ── Core logical source properties ───────────────────────────────────────────
RML_LOGICAL_SOURCE        = f"{RML_NAMESPACE}logicalSource"
RML_SOURCE                = f"{RML_NAMESPACE}source"
RML_QUERY                 = f"{RML_NAMESPACE}query"
RML_ITERATOR              = f"{RML_NAMESPACE}iterator"
RML_REFERENCE_FORMULATION = f"{RML_NAMESPACE}referenceFormulation"
RML_LOGICAL_TABLE         = f"{RML_NAMESPACE}logicalTable"
RML_TABLE_NAME            = f"{RML_NAMESPACE}tableName"

# ── Core map properties ───────────────────────────────────────────────────────
RML_PARENT_TRIPLES_MAP   = f"{RML_NAMESPACE}parentTriplesMap"
RML_SUBJECT_MAP          = f"{RML_NAMESPACE}subjectMap"
RML_PREDICATE_MAP        = f"{RML_NAMESPACE}predicateMap"
RML_OBJECT_MAP           = f"{RML_NAMESPACE}objectMap"
RML_GRAPH_MAP            = f"{RML_NAMESPACE}graphMap"
RML_DATATYPE_MAP         = f"{RML_NAMESPACE}datatypeMap"
RML_LANGUAGE_MAP         = f"{RML_NAMESPACE}languageMap"
RML_PREDICATE_OBJECT_MAP = f"{RML_NAMESPACE}predicateObjectMap"
RML_SUBJECT_SHORTCUT     = f"{RML_NAMESPACE}subject"
RML_PREDICATE_SHORTCUT   = f"{RML_NAMESPACE}predicate"
RML_OBJECT_SHORTCUT      = f"{RML_NAMESPACE}object"
RML_GRAPH_SHORTCUT       = f"{RML_NAMESPACE}graph"
RML_DATATYPE_SHORTCUT    = f"{RML_NAMESPACE}datatype"
RML_LANGUAGE_SHORTCUT    = f"{RML_NAMESPACE}language"

# ── Term map value types ──────────────────────────────────────────────────────
RML_CONSTANT  = f"{RML_NAMESPACE}constant"
RML_TEMPLATE  = f"{RML_NAMESPACE}template"
RML_REFERENCE = f"{RML_NAMESPACE}reference"

# ── Other core properties ─────────────────────────────────────────────────────
RML_CLASS          = f"{RML_NAMESPACE}class"
RML_CHILD          = f"{RML_NAMESPACE}child"
RML_PARENT         = f"{RML_NAMESPACE}parent"
RML_JOIN_CONDITION = f"{RML_NAMESPACE}joinCondition"
RML_SQL_QUERY      = f"{RML_NAMESPACE}sqlQuery"
RML_SQL_VERSION    = f"{RML_NAMESPACE}sqlVersion"
RML_TERM_TYPE      = f"{RML_NAMESPACE}termType"

# ── Term types ────────────────────────────────────────────────────────────────
RML_DEFAULT_GRAPH = f"{RML_NAMESPACE}defaultGraph"
RML_IRI           = f"{RML_NAMESPACE}IRI"
RML_LITERAL       = f"{RML_NAMESPACE}Literal"
RML_BLANK_NODE    = f"{RML_NAMESPACE}BlankNode"

# ── SQL / query language tokens ───────────────────────────────────────────────
RML_SQL2008    = f"{RML_NAMESPACE}SQL2008"
RML_CYPHER     = f"{RML_NAMESPACE}Cypher"

# ── Reference formulations ────────────────────────────────────────────────────
RML_CSV        = f"{RML_NAMESPACE}CSV"
RML_JSONPATH   = f"{RML_NAMESPACE}JSONPath"
RML_XPATH      = f"{RML_NAMESPACE}XPath"
RML_GEOPARQUET = f"{RML_NAMESPACE}GeoParquet"
RML_SHP        = f"{RML_NAMESPACE}Shapefile"
