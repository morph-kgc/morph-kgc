__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Stage 3 — Term Serialisation
=============================
Converts raw source values in a DataFrame into N-Triples-serialized RDF terms
(IRIs, blank nodes, typed literals, language-tagged strings).

Handles all term-map types: rml:template, rml:constant, rml:reference,
rml:execution (FNML), and — new in RML 1.2 — triple terms and directional
language-tagged strings.

Public API
----------
materialize_terms(data, rule, rml_mapping, config, columns_alias='') -> pd.DataFrame
"""

import pandas as pd

from falcon.uri import encode_value
from urllib.parse import quote

from ...constants import (
    RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE, RML_EXECUTION,
    RML_PARENT_TRIPLES_MAP, RML_REIFYING_MAP, RML_TRIPLE_TERM_MAP,
    RML_IRI, RML_LITERAL, RML_BLANK_NODE,
    RML_LANGUAGE_MAP, RML_DATATYPE_MAP, VALID_DIRECTIONS,
    XSD_BOOLEAN, XSD_DATETIME, XSD_INTEGER,
)
from ...mapping.model import RMLMapping, RMLRule
from ...utils import get_references_in_template
from ...functions import execute_fnml


# ── Literal escaping ──────────────────────────────────────────────────────────

def _escape_literal(series: pd.Series, datatype: str, config) -> pd.Series:
    if datatype == XSD_BOOLEAN:
        series = series.str.lower()
    elif datatype == XSD_DATETIME:
        series = series.str.replace(" ", "T", regex=False)
    elif datatype == XSD_INTEGER:
        series = series.astype(float).astype(int).astype(str)
    series = (
        series
        .str.replace("\\", "\\\\", regex=False)
        .str.replace("\n", "\\n",   regex=False)
        .str.replace("\r", "\\r",   regex=False)
        .str.replace('"',  '\\"',   regex=False)
    )
    for char in config.literal_escaping_chars:
        if char not in ('"', "\n", "\\", "\r"):
            esc = f"\\{char}" if char in ("\n", "\r", "\t", "\b", "\f") else f"\\\\{char}"
            series = series.str.replace(char, esc, regex=False)
    return series


# ── Core term materialisation ─────────────────────────────────────────────────

def _apply_template(
    data: pd.DataFrame,
    template: str,
    expression_type: str,
    config,
    position: str,
    columns_alias: str = "",
    termtype: str = "",
    datatype: str = "",
) -> pd.DataFrame:
    """Write serialized RDF term into data[position] for template/reference/constant maps."""
    if expression_type == RML_REFERENCE:
        template = f"{{{template}}}"

    references = get_references_in_template(template)
    template = template.replace("\\{", "{").replace("\\}", "}")
    data[position] = ""

    for ref in references:
        data["_ref"] = data[columns_alias + ref]

        if termtype.strip() == RML_IRI and expression_type == RML_TEMPLATE:
            safe = config.safe_percent_encoding
            if safe:
                data["_ref"] = data["_ref"].apply(lambda x: quote(x, safe=safe))
            else:
                data["_ref"] = data["_ref"].apply(encode_value)
        elif termtype.strip() == RML_LITERAL:
            data["_ref"] = _escape_literal(data["_ref"], datatype, config)

        parts = template.split("{" + ref + "}")
        data[position] = data[position] + parts[0] + data["_ref"]
        template = ("{" + ref + "}").join(parts[1:])

    if template:
        data[position] = data[position] + template

    t = termtype.strip()
    if t == RML_IRI:
        data[position] = "<" + data[position] + ">"
    elif t == RML_BLANK_NODE:
        data[position] = "_:" + data[position]
    elif t == RML_LITERAL:
        data[position] = '"' + data[position] + '"'

    return data


def _apply_parent_triples_map(
    data: pd.DataFrame,
    tm,                            # TermMap with map_type == RML_PARENT_TRIPLES_MAP
    rml_mapping: RMLMapping,
    config,
    position: str,
    columns_alias: str = "",
    datatype: str = "",
) -> pd.DataFrame:
    """
    Resolve a referencing object map: materialize the parent triples map's
    subject term directly against *data*, which — when a join condition is
    present — has already been merged with the parent's raw columns
    (prefixed by *columns_alias*, e.g. "parent_") in materialize_rule().
    No additional loading or merging happens here.
    """
    parent_rule = rml_mapping.get_rule(tm.map_value)

    return _write_term(
        data, parent_rule.subject, position, rml_mapping, config,
        columns_alias=columns_alias,
        termtype_override=parent_rule.subject.term_type,
        datatype=datatype,
    )


def _apply_fnml(
    data: pd.DataFrame,
    fnml_execution: str,
    rml_mapping: RMLMapping,
    config,
    position: str,
    termtype: str = RML_LITERAL,
    datatype: str = "",
) -> pd.DataFrame:
    """Write serialised RDF term into data[position] for FNML execution maps."""
    execution = rml_mapping.fnml_executions.get(fnml_execution)
    if execution is None:
        raise KeyError(f'Function execution {fnml_execution!r} not found in the mapping.')

    data = execute_fnml(data, execution, config)
    data[fnml_execution] = data[fnml_execution].astype(str)

    t = termtype.strip()
    if t == RML_LITERAL:
        data[fnml_execution] = _escape_literal(data[fnml_execution], datatype, config)
        data[position] = '"' + data[fnml_execution] + '"'
    elif t == RML_IRI:
        data[fnml_execution] = data[fnml_execution].apply(str.strip)
        data[position] = "<" + data[fnml_execution] + ">"
    elif t == RML_BLANK_NODE:
        data[position] = "_:" + data[fnml_execution]

    return data


def _write_term(
    data: pd.DataFrame,
    tm,                           # TermMap | None
    position: str,
    rml_mapping: RMLMapping,
    config,
    columns_alias: str = "",
    termtype_override: str | None = None,
    datatype: str = "",
) -> pd.DataFrame:
    """Dispatch to _apply_template, _apply_fnml, or _apply_parent_triples_map
    based on tm.map_type."""
    if tm is None:
        return data
    termtype = termtype_override if termtype_override is not None else (tm.term_type or "")
    if tm.map_type in (RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE):
        return _apply_template(
            data, tm.map_value, tm.map_type, config, position,
            columns_alias=columns_alias, termtype=termtype, datatype=datatype,
        )
    elif tm.map_type == RML_PARENT_TRIPLES_MAP:
        return _apply_parent_triples_map(
            data, tm, rml_mapping, config, position,
            columns_alias=columns_alias, datatype=datatype,
        )
    elif tm.map_type == RML_EXECUTION:
        return _apply_fnml(
            data, tm.map_value, rml_mapping, config, position,
            termtype=termtype, datatype=datatype,
        )
    return data


# ── RML 1.2 terms ─────────────────────────────────────────────────────────────

def _write_annotation(
    data: pd.DataFrame,
    map_type: str | None,
    map_value: str | None,
    rml_mapping: RMLMapping,
    config,
    position: str,
    termtype: str = "",
) -> pd.DataFrame:
    """Materialize a language, datatype or direction term map into its own column."""
    if map_type in (RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE):
        return _apply_template(
            data, map_value, map_type, config, position, termtype=termtype,
        )
    if map_type == RML_EXECUTION:
        return _apply_fnml(
            data, map_value, rml_mapping, config, position, termtype=termtype,
        )
    return data


def _apply_direction(
    data: pd.DataFrame,
    om,                            # TermMap carrying rml:directionMap
    rml_mapping: RMLMapping,
    config,
    position: str,
    depth: int,
) -> pd.DataFrame:
    """
    Append the base direction of a directional language-tagged string.

    A constant direction is rejected while the mapping is parsed; a reference-
    or template-valued one can only be checked here, once the row is known.
    """
    direction_column = f"_direction{depth}"
    data = _write_annotation(
        data, om.direction_map_type, om.direction_map_value,
        rml_mapping, config, direction_column,
    )

    invalid = set(data[direction_column].unique()) - set(VALID_DIRECTIONS)
    if invalid:
        raise ValueError(
            f"Invalid base direction {sorted(invalid)} generated by rml:directionMap. "
            f"A direction must be one of {' or '.join(VALID_DIRECTIONS)}."
        )

    data[position] = data[position] + "--" + data[direction_column]
    return data.drop(columns=[direction_column], errors="ignore")


def _apply_triple_term_map(
    data: pd.DataFrame,
    chain: list[RMLRule],
    aliases: list[str],
    rml_mapping: RMLMapping,
    config,
    position: str,
    depth: int = 0,
) -> pd.DataFrame:
    """
    Write an RDF 1.2 triple term ``<<( s p o )>>`` into ``data[position]``.

    *chain* holds the base rules the triple-term map resolves to: the first
    supplies this triple term, and any further ones supply the nested triple
    term that is its object. *aliases* gives, for each of them, the column
    prefix its term maps read: empty for the rule's own logical iteration, and
    the prefix of a joined logical source where a join condition brought one
    in. Graph maps of the base triples map take no part: they only place its
    standalone triples.
    """
    if not chain:
        raise ValueError("A triple-term map resolved to no base rule.")

    base_rule     = chain[0]
    columns_alias = aliases[0]
    subject_column   = f"_tt{depth}_subject"
    predicate_column = f"_tt{depth}_predicate"
    object_column    = f"_tt{depth}_object"

    data = _write_term(
        data, base_rule.subject, subject_column, rml_mapping, config,
        columns_alias=columns_alias,
    )
    data = _write_term(
        data, base_rule.predicate, predicate_column, rml_mapping, config,
        columns_alias=columns_alias, termtype_override=RML_IRI,
    )
    data = _write_object_term(
        data, base_rule.object_, object_column, rml_mapping, config,
        columns_alias=columns_alias,
        triple_term_chain=chain[1:],
        triple_term_aliases=aliases[1:],
        depth=depth + 1,
    )

    data[position] = (
        "<<( " + data[subject_column]
        + " " + data[predicate_column]
        + " " + data[object_column] + " )>>"
    )
    return data.drop(
        columns=[subject_column, predicate_column, object_column], errors="ignore",
    )


def _write_object_term(
    data: pd.DataFrame,
    om,                            # TermMap | None
    position: str,
    rml_mapping: RMLMapping,
    config,
    columns_alias: str = "",
    triple_term_chain: list[RMLRule] | None = None,
    triple_term_aliases: list[str] | None = None,
    depth: int = 0,
) -> pd.DataFrame:
    """
    Write an object term into ``data[position]``, annotations included.

    Handles the plain term maps, a triple-term map when *triple_term_chain*
    names the base rules it resolves to, and the language, datatype and base
    direction that may follow a literal value.
    """
    if om is None:
        return data

    if om.map_type == RML_TRIPLE_TERM_MAP:
        return _apply_triple_term_map(
            data, triple_term_chain, triple_term_aliases,
            rml_mapping, config, position, depth=depth,
        )

    data = _write_term(
        data, om, position, rml_mapping, config,
        columns_alias=columns_alias,
        datatype=om.lang_datatype_map_value or "",
    )

    if om.lang_datatype == RML_LANGUAGE_MAP:
        language_column = f"_lang{depth}"
        data = _write_annotation(
            data, om.lang_datatype_map_type, om.lang_datatype_map_value,
            rml_mapping, config, language_column,
        )
        data[position] = data[position] + "@" + data[language_column]
        if om.direction_map_type is not None:
            data = _apply_direction(data, om, rml_mapping, config, position, depth)
        data = data.drop(columns=[language_column], errors="ignore")

    elif om.lang_datatype == RML_DATATYPE_MAP:
        datatype_column = f"_dtype{depth}"
        data = _write_annotation(
            data, om.lang_datatype_map_type, om.lang_datatype_map_value,
            rml_mapping, config, datatype_column, termtype=RML_IRI,
        )
        data[position] = data[position] + "^^" + data[datatype_column]
        data = data.drop(columns=[datatype_column], errors="ignore")

    return data


# ── Public stage entry point ──────────────────────────────────────────────────

def materialize_terms(
    data: pd.DataFrame,
    rule: RMLRule,
    rml_mapping: RMLMapping,
    config,
    columns_alias: str = "",
    triple_term_chain: list[RMLRule] | None = None,
    triple_term_aliases: list[str] | None = None,
) -> pd.DataFrame:
    """
    Materialize subject / predicate / object / graph terms for every row in
    *data* and return the DataFrame with those columns populated.

    *triple_term_chain* is set when the rule's object map is a triple-term map:
    it names the base rules that this particular triple term is built from, and
    *triple_term_aliases* the column prefix each of them reads.
    """
    data = _write_term(data, rule.subject, "subject", rml_mapping, config)
    data = _write_term(data, rule.predicate, "predicate", rml_mapping, config,
                       termtype_override=RML_IRI)
    data = _write_object_term(
        data, rule.object_, "object", rml_mapping, config,
        columns_alias=columns_alias,
        triple_term_chain=triple_term_chain,
        triple_term_aliases=triple_term_aliases,
    )
    return data
