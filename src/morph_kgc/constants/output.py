__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""Output format constants and configuration option names."""

# ── RDF serialisation formats ─────────────────────────────────────────────────
NTRIPLES = "N-TRIPLES"
NQUADS   = "N-QUADS"
JELLY    = "JELLY"

VALID_OUTPUT_FORMATS = [NTRIPLES, NQUADS, JELLY]

OUTPUT_FORMAT_FILE_EXTENSION = {
    NTRIPLES: ".nt",
    NQUADS:   ".nq",
    JELLY:    ".jelly",
}

# ── Configuration option names (used by config.py) ────────────────────────────
OUTPUT_FORMAT      = "output_format"
OUTPUT_FILE        = "output_file"
OUTPUT_DIR         = "output_dir"

# ── Logging ───────────────────────────────────────────────────────────────────
VALID_LOGGING_LEVEL = ["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LOGGING_LEVEL       = "logging_level"
LOGGING_FILE        = "logging_file"
