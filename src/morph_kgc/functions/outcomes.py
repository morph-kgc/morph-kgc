from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Outcomes of stateful functions
==============================
What a stateful function reports about the values it is called with: how many
rows it handled with each outcome, and records of the values worth a look (a
value a reconciliation function matched to no entity, or to several), which
the run gathers into a report once every mapping rule is materialized.

A function declaring an ``outcomes_parameter`` (see
:mod:`morph_kgc.functions.bif_decorator`) is handed an :class:`Outcomes` on
every call, which knows the execution and the mapping rule it is called for.
Each process appends what its calls report to a file of its own in the state
directory of the run, so that worker processes never interleave their lines
and nothing is passed between processes, and writes each record once. Once
every rule is materialized, whether or not the run fails, the main process
reads the files back (:func:`report_outcomes`): it logs a summary for each
execution, and writes one line per distinct (execution, value, outcome) to the
file the 'reconciliation_report' option names, whatever the number of processes
and of rows holding the value. The values themselves are never logged.

A record gives the outcome, the value as read and as looked up, the function
and its execution, the triples map, what the value was matched against (a
resource, or an endpoint and the hash of its query), the entities it matched
when several, and the source of the triples map and the mapping files
declaring it. A value an
execution handles for several triples maps (or against several resources) is
one record, whose field names each of them, joined by ', '.

A function reports the values it is called with, which are those of the rows
that reach the term it computes: not those of a row any referenced column of
the rule is missing in (na_values), nor those of a row an earlier term of the
rule yields no value for (a subject reconciled to no entity, for instance,
before its object is).

Lifecycle
---------
CURRENT_RULE
    The mapping rule being materialized, set by the pipeline.
Outcomes(execution_id, function_iri, config)
    What one call of a function reports, written by :meth:`Outcomes.flush`.
report_outcomes(state_dir, config)
    The summary and the report of the run, from what its processes wrote.
"""

import glob
import json
import logging
import os
import threading
from contextvars import ContextVar
from pathlib import Path

from ..constants import LOGGING_NAMESPACE

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

# The mapping rule whose triples are being materialized, which the functions a
# rule executes report their outcomes for: set by the pipeline for each rule.
CURRENT_RULE: ContextVar = ContextVar("morph_kgc_current_rule", default=None)

# The files the processes of a run write their outcomes to, in its state
# directory, one per process.
OUTCOMES_FILE_PREFIX = "outcomes-"
OUTCOMES_FILE_SUFFIX = ".jsonl"

# How the outcomes of reconciliation are named in the summary of an execution.
OUTCOME_NAMES = {
    "MATCH": "matched",
    "NO_MATCH": "unmatched",
    "MULTIPLE_MATCH": "ambiguous",
}

# The fields of a record, in the order they are written: those a function
# records (what the value was matched against) go between the two.
LEADING_FIELDS = ("outcome", "value", "normalized_value", "function_iri", "execution_id", "triples_map")
TRAILING_FIELDS = ("candidates", "source", "mapping")
# The fields whose values in the copies of a record (in several mapping rules,
# rows or processes) are gathered: joined, sorted, or united.
JOINED_FIELDS = ("triples_map", "source", "mapping", "resource", "endpoint", "query_hash")
JOIN_SEPARATOR = ", "
UNITED_FIELDS = ("candidates",)

# The records this process has written, by state directory, so that each is
# written once whatever the number of rows and calls holding it.
_WRITTEN: dict[str, set] = {}
_WRITTEN_LOCK = threading.Lock()


class Outcomes:
    """
    What one call of a stateful function reports: the number of rows of each
    outcome, and records of some of them. They are written to the outcomes file
    of this process by :meth:`flush`, which the function executor calls once
    the function returns or fails.
    """

    def __init__(self, execution_id: str, function_iri: str, config) -> None:
        self.execution_id = execution_id
        self.function_iri = function_iri
        self._state_dir = getattr(config, "state_dir", "") or ""
        rule = CURRENT_RULE.get()
        # The rule the function is called for, None outside the pipeline.
        self._rule = None if rule is None else {
            "triples_map": rule.triples_map_id,
            "source": rule.logical_source.value,
            "data_source": rule.logical_source.config_section_name,
            "mapping": [path for path in rule.mapping_files.split("\n") if path],
        }
        self._counts: dict[str, int] = {}
        self._records: list[dict] = []

    def count(self, outcome: str, rows: int = 1) -> None:
        """Count *rows* handled with *outcome*."""
        self._counts[outcome] = self._counts.get(outcome, 0) + rows

    def record(self, outcome: str, key, **fields) -> None:
        """
        Record a value handled with *outcome*, told apart from the others of
        the execution by *key*, described by *fields* (both JSON-serializable,
        anything else written as a string). Each copy is written once by each
        process: one per mapping rule and set of fields, which the report
        gathers into a single record (see :data:`JOINED_FIELDS`).
        """
        identity = json.dumps(
            [self.execution_id, outcome, key, self._rule, fields], sort_keys=True, default=str,
        )
        with _WRITTEN_LOCK:
            written = _WRITTEN.setdefault(self._state_dir, set())
            if identity in written:
                return
            written.add(identity)
        self._records.append({
            "key": [self.execution_id, outcome, key],
            "rule": self._rule,
            "record": {
                "outcome": outcome,
                **fields,
                "function_iri": self.function_iri,
                "execution_id": self.execution_id,
            },
        })

    def flush(self) -> None:
        """Append what the call reported to the outcomes file of this process."""
        if not self._state_dir or not (self._counts or self._records):
            return
        lines = [
            json.dumps({
                "counts": self._counts,
                "execution_id": self.execution_id,
                "function_iri": self.function_iri,
            }, default=str)
        ]
        lines += [json.dumps(record, default=str) for record in self._records]
        path = os.path.join(
            self._state_dir, f"{OUTCOMES_FILE_PREFIX}{os.getpid()}{OUTCOMES_FILE_SUFFIX}",
        )
        # One write of whole lines by a single process: ASCII only, so that any
        # value, a lone surrogate included, is written.
        with open(path, "a", encoding="ascii") as outcomes_file:
            outcomes_file.write("\n".join(lines) + "\n")
        self._counts, self._records = {}, []


def forget_outcomes(state_dir: str) -> None:
    """Forget the records this process has written for the run in *state_dir*."""
    with _WRITTEN_LOCK:
        _WRITTEN.pop(state_dir, None)


def report_outcomes(state_dir: str, config) -> None:
    """
    Read back what the processes of the run in *state_dir* wrote, log a summary
    for each execution, and write the de-duplicated records to the
    'reconciliation_report' file, when the configuration names one: a line per
    distinct (execution, value, outcome), in JSON. The file is
    written (empty if need be) whenever it is named, so that once the rules are
    materialized it never holds the report of an earlier run. A line a process
    could not finish writing (a worker terminated as the run is aborted) is
    left out.
    """
    counts: dict[tuple[str, str], dict[str, int]] = {}
    records: dict[str, dict] = {}
    mappings = {
        name: list(source.mappings) for name, source in getattr(config, "data_sources", {}).items()
    }
    files = sorted(glob.glob(os.path.join(glob.escape(state_dir), f"{OUTCOMES_FILE_PREFIX}*"))) if state_dir else []
    for path in files:
        with open(path, encoding="ascii") as outcomes_file:
            for number, line in enumerate(outcomes_file, start=1):
                try:
                    entry = json.loads(line)
                except ValueError:
                    LOGGER.warning(f"Line {number} of '{path}' is not a whole outcome, it is left out.")
                    continue
                if "counts" in entry:
                    execution = counts.setdefault((entry["execution_id"], entry["function_iri"]), {})
                    for outcome, rows in entry["counts"].items():
                        execution[outcome] = execution.get(outcome, 0) + rows
                    continue
                key = json.dumps(entry["key"], sort_keys=True)
                _gather(records.setdefault(key, {}), entry["record"], entry["rule"], mappings)

    records = {key: _record(copies) for key, copies in records.items()}
    distinct: dict[tuple[str, str], int] = {}
    for record in records.values():
        identity = (record["execution_id"], record["outcome"])
        distinct[identity] = distinct.get(identity, 0) + 1
    for (execution_id, function_iri), execution in sorted(counts.items()):
        LOGGER.info(_summary(execution_id, function_iri, execution, distinct))

    report = getattr(config, "reconciliation_report", "")
    if not report:
        return
    Path(report).parent.mkdir(parents=True, exist_ok=True)
    with open(report, "w", encoding="utf-8", errors="backslashreplace", newline="\n") as report_file:
        for key in sorted(records):
            report_file.write(json.dumps(records[key], ensure_ascii=False) + "\n")
    LOGGER.info(f"Reconciliation report written to '{report}': {len(records)} record(s).")


def _gather(copies: dict, written: dict, rule: dict | None, mappings: dict) -> None:
    """
    Add to *copies* (field -> its values) a copy of a record, *written* for the
    mapping *rule*, whose triples map, source and mapping files it is given.
    """
    fields = dict(written)
    if rule is not None:
        fields["triples_map"] = rule["triples_map"]
        fields["source"] = rule["source"]
        # The files declaring the triples map, or else those of its section.
        fields["mapping"] = rule.get("mapping") or mappings.get(rule["data_source"], [])
    for field, value in fields.items():
        values = copies.setdefault(field, [])
        if field == "mapping" or field in UNITED_FIELDS:
            values.extend(value)
        else:
            values.append(value)


def _record(copies: dict) -> dict:
    """The record of a value from its *copies* (field -> its values), in order."""
    record = {}
    for field, values in copies.items():
        if field in JOINED_FIELDS:
            record[field] = JOIN_SEPARATOR.join(sorted({str(value) for value in values}))
        elif field in UNITED_FIELDS:
            record[field] = sorted(set(values))
        else:
            # The same in every copy, as the key of the record determines it.
            record[field] = values[0]
    middle = [field for field in record if field not in LEADING_FIELDS + TRAILING_FIELDS]
    return {
        field: record[field]
        for field in (*LEADING_FIELDS, *middle, *TRAILING_FIELDS)
        if field in record
    }


def _summary(execution_id: str, function_iri: str, execution: dict, distinct: dict) -> str:
    """The summary of what *execution* reported, with its distinct records."""
    # Every count of the outcomes of reconciliation, zero ones included, and
    # those of any other outcome a function reports.
    parts = []
    for outcome in [*OUTCOME_NAMES, *sorted(set(execution) - set(OUTCOME_NAMES))]:
        part = f"{execution.get(outcome, 0)} {OUTCOME_NAMES.get(outcome, outcome)}"
        if (execution_id, outcome) in distinct:
            part += f" ({distinct[(execution_id, outcome)]} distinct)"
        parts.append(part)
    return (
        f"Execution '{execution_id}' of function '{function_iri}' handled "
        f"{sum(execution.values())} value(s): {', '.join(parts)}."
    )
