import gdl_common as C

TYPES = {"string", "int", "bigint", "decimal", "date", "timestamp"}
FORMATS = {"mysqldump", "psv", "jsonl", "json_array", "binary"}


def test_every_configured_entity_has_a_contract_in_order():
    cfg = C.load_json("config/pipeline.json")
    assert list(C.contracts()) == cfg["entities"]


def test_contracts_are_well_formed():
    for entity, c in C.contracts().items():
        assert c["entity"] == entity
        assert c["source"] in C.SOURCES, entity
        assert c["format"] in FORMATS, entity
        names = [col["name"] for col in c["columns"]]
        assert len(names) == len(set(names)), entity
        for col in c["columns"]:
            assert col["type"] in TYPES, (entity, col["name"])
            assert col.get("severity", "warn") in ("warn", "reject"), (entity, col["name"])
        for k in c["key"]:
            assert k in names, (entity, k)
        if c.get("control_field"):
            assert c["control_field"] in names, entity
        if c["format"] == "mysqldump":
            assert c.get("table"), entity


def test_cdc_images_point_at_contracts():
    cdc = C.contracts()["cbs_cdc_event"]
    for table, entity in cdc["image_contracts"].items():
        assert entity in C.contracts(), table
