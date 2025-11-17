__author__ = "Volodymyr Lapkin"
__credits__ = ["Volodymyr Lapkin"]
__license__ = "Apache-2.0"
__maintainer__ = "Volodymyr Lapkin"
__email__ = "lapkinvladimir@gmail.com"

from pathlib import Path
import subprocess, sys
import pytest
import rdflib
import morph_kgc

pytest.importorskip("pyjelly")

HERE = Path(__file__).parent
RDF_TYPE = rdflib.URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
EX_NAME  = rdflib.URIRef("http://example.com/name")

def _g_from_nt(p: Path) -> rdflib.Graph:
    g = rdflib.Graph()
    g.parse(p.as_posix(), format="nt")
    return g

def _g_from_jelly(p: Path) -> rdflib.Graph:
    g = rdflib.Graph()
    g.parse(p.as_posix(), format="jelly")
    return g

def _assert_semantics(g: rdflib.Graph):
    names = sorted([
        (str(s), str(o))
        for (s, p, o) in g
        if str(p).rstrip('/').endswith('/name') and isinstance(o, rdflib.Literal)
    ])
    assert ("http://example.com/person/1", "name1") in names
    assert ("http://example.com/person/2", "name2") in names

    ex_type = rdflib.URIRef("http://example.com/type")
    acceptable_preds = {RDF_TYPE, ex_type}

    def is_person_iri(x: rdflib.term.Identifier) -> bool:
        return isinstance(x, rdflib.URIRef) and str(x).rstrip("/").endswith("/Person")

    type_triples = [(s, p, o) for (s, p, o) in g if p in acceptable_preds and is_person_iri(o)]
    subj_set = {str(s) for (s, _, _) in type_triples}
    assert "http://example.com/person/1" in subj_set
    assert "http://example.com/person/2" in subj_set
    assert len(type_triples) >= 2


def test_issue_350_jelly_output_equals_expected(monkeypatch):
    monkeypatch.chdir(HERE)

    config = (
        "[CONFIGURATION]\n"
        "output_format=JELLY\n"
        "[DataSource1]\n"
        f"mappings={(HERE / 'mapping.ttl').as_posix()}\n"
    )

    graph = morph_kgc.materialize(config)

    output_path = HERE / "test_output.jelly"
    graph.serialize(destination=output_path, format="jelly")

    g_out = _g_from_jelly(output_path)
    g_exp = _g_from_nt(HERE / "output.nt")

    _assert_semantics(g_out)
    assert len(g_out) == len(g_exp)

    output_path.unlink()

def test_issue_350_jelly_file_created_and_parsable_cli(tmp_path: Path):
    for name in ("data.csv", "mapping.ttl", "config.ini"):
        (tmp_path / name).write_text((HERE / name).read_text(encoding="utf-8"), encoding="utf-8")

    subprocess.check_call([sys.executable, "-m", "morph_kgc", "config.ini"], cwd=tmp_path)

    jelly_file = tmp_path / "kg.jelly"
    assert jelly_file.exists(), "kg.jelly was not created by CLI run"

    g_out = _g_from_jelly(jelly_file)
    _assert_semantics(g_out)

def test_issue_350_triples_to_file_jelly_guard(tmp_path: Path):
    from morph_kgc.args_parser import load_config_from_argument
    from morph_kgc.utils import triples_to_file

    cfg_str = (
        "[CONFIGURATION]\n"
        f"output_file={(tmp_path / 'kg.jelly').as_posix()}\n"
        "output_format=JELLY\n"
        "[DataSource1]\n"
        f"mappings={(HERE / 'mapping.ttl').as_posix()}\n"
    )
    cfg = load_config_from_argument(cfg_str)

    with pytest.raises(Exception):
        triples_to_file(["<s> <p> <o>"], cfg)

def test_issue_350_nt_cli_semantics(tmp_path: Path):
    (tmp_path / "data.csv").write_text((HERE / "data.csv").read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "mapping.ttl").write_text((HERE / "mapping.ttl").read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "config.nt.ini").write_text(
        "[CONFIGURATION]\n"
        "output_file=kg.nt\n"
        "output_format=N-TRIPLES\n"
        "[DataSource1]\n"
        f"mappings={(tmp_path / 'mapping.ttl').as_posix()}\n",
        encoding="utf-8"
    )

    subprocess.check_call([sys.executable, "-m", "morph_kgc", "config.nt.ini"], cwd=tmp_path)

    nt_file = tmp_path / "kg.nt"
    assert nt_file.exists()

    g_out = _g_from_nt(nt_file)
    g_exp = _g_from_nt(HERE / "output.nt")
    _assert_semantics(g_out)
    assert len(g_out) == len(g_exp)
