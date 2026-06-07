import os
import morph_kgc
from morph_kgc.constants import OUTPUT_FORMAT_FILE_EXTENSION


def materialize_graph(mapping_path, db_url, na_values=",nan,None"):
    config_str = (
        f"[CONFIGURATION]\n"
        f"na_values={na_values}\n"
        f"[DataSource]\n"
        f"mappings={mapping_path}\n"
        f"db_url={db_url}\n"
    )
    return morph_kgc.materialize(config_str)


def serialize_graph(graph, output_dir, output_file, output_format):
    ext = OUTPUT_FORMAT_FILE_EXTENSION.get(output_format, ".nt").lstrip(".")
    rdflib_fmt = output_format.lower().replace("-", "")
    output_path = os.path.join(output_dir, f"{output_file}.{ext}")
    graph.serialize(destination=output_path, format=rdflib_fmt)
    return output_path
