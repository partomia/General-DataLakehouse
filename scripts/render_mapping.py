#!/usr/bin/env python3
"""
Render model/source_mapping.csv into docs/BANKING_MODEL.md, between the
<!-- mapping:start --> and <!-- mapping:end --> markers: one table per gold table, a row
per column (source system, entity, field, transform, rule). tests/test_docs.py fails when
the document is out of date.

  python scripts/render_mapping.py
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "BANKING_MODEL.md"
START, END = "<!-- mapping:start -->", "<!-- mapping:end -->"


def cell(v: str | None) -> str:
    return (v or "").replace("|", "\\|")


def rendered() -> str:
    tables: dict[str, list[dict]] = {}
    for r in csv.DictReader((ROOT / "model" / "source_mapping.csv").open()):
        tables.setdefault(r["target_table"], []).append(r)
    out = [START, ""]
    for t, rows in tables.items():
        out += [f"#### `{t}`", "", "| Column | Source | Entity | Field | Transform | Rule |", "|---|---|---|---|---|---|"]
        out += [f"| `{r['target_column']}` | {cell(r['source_system'])} | {cell(r['source_entity'])} | "
                f"{cell(r['source_field'])} | {cell(r['transform_type'])} | {cell(r['rule'])} |" for r in rows]
        out.append("")
    out.append(END)
    return "\n".join(out)


def updated(text: str) -> str:
    return re.sub(re.escape(START) + ".*?" + re.escape(END), lambda _: rendered(), text, flags=re.S)


def main() -> int:
    DOC.write_text(updated(DOC.read_text()))
    print(f"rendered model/source_mapping.csv into {DOC.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
