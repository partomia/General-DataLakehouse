import gdl_common as C

DUMP = """-- MySQL dump
CREATE TABLE `customer` (
  `cif` int NOT NULL,
  `first_name` varchar(60) DEFAULT NULL,
  `note` text,
  PRIMARY KEY (`cif`)
) ENGINE=InnoDB;
INSERT INTO `customer` VALUES (1,'Asha','it''s, ok'),(2,NULL,'line\\nbreak (x)'),(3,'O\\'Neil','');
INSERT INTO `other` VALUES (9,'x','y');
"""


def test_dump_columns_follow_create_table():
    assert C.dump_columns(DUMP, "customer") == ["cif", "first_name", "note"]


def test_parse_dump_handles_quotes_escapes_and_null():
    rows = list(C.parse_dump("s3a://b/landing/cbs/x.sql", DUMP, "customer", ["note", "cif", "first_name", "missing"]))
    assert rows == [
        ("x.sql", 1, "it's, ok", "1", "Asha", None),
        ("x.sql", 2, "line\nbreak (x)", "2", None, None),
        ("x.sql", 3, "", "3", "O'Neil", None),
    ]


def test_quoted_null_is_a_string():
    assert C._tuples("(1,'NULL',NULL)") == [["1", "NULL", None]]


PSV = "id|amount|note\n1|10.50|a\n2||b\nT|3|10.50\n"


def test_parse_psv_maps_header_and_empty_to_none():
    rows = list(C.parse_psv("f.csv", PSV, ["amount", "id", "absent"]))
    assert rows == [("f.csv", 2, "10.50", "1", None), ("f.csv", 3, None, "2", None)]


def test_psv_stats_reads_trailer():
    assert C.psv_stats("dir/f.csv", PSV) == ("f.csv", "id|amount|note", 2, 3, 10.5)


def test_parse_jsonl_flags_malformed_lines():
    rows = list(C.parse_jsonl("p.jsonl", '{"a":1}\n\n{"a":\n'))
    assert rows[0] == ("p.jsonl", 1, '{"a":1}', None)
    assert rows[1][1] == 3 and rows[1][3]


def test_parse_json_array():
    rows = list(C.parse_json_array("c.json", '[{"id":1},{"id":2}]'))
    assert [r[2] for r in rows] == ['{"id":1}', '{"id":2}']
    assert list(C.parse_json_array("c.json", "[{"))[0][3]


def test_error_summary_keeps_the_innermost_python_error():
    wrapped = Exception("PythonException:\n  An exception was thrown\nTraceback ...\n"
                        "RuntimeError: simulated failure in task for partition 1\n")
    assert C.error_summary(wrapped) == "RuntimeError: simulated failure in task for partition 1"
    assert C.error_summary(ValueError("bad")) == "ValueError: bad"


def test_names_and_batch_id():
    n = C.Names("rsingh_gdl")
    assert n.db("silver") == "rsingh_gdl_silver"
    assert n.t("gold", "dim_party") == "rsingh_gdl_gold.dim_party"
    assert C.batch_id(C.parse_date("2026-09-23")) == "B20260923"
