import importlib.util

from conftest import ROOT


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_banking_model_mapping_tables_match_the_csv():
    r = load_script("render_mapping")
    text = r.DOC.read_text()
    assert r.updated(text) == text, "docs/BANKING_MODEL.md is out of date: python scripts/render_mapping.py"
