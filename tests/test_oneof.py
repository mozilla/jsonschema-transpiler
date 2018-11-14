import copy


def test_oneof_atomic():
    oneof = {"oneOf": [{"type": "integer"}, {"type": "integer"}]}
    expected = {"type": "INTEGER", "mode": "REQUIRED"}
    assert bq_schema(oneof) == expected


def test_oneof_atomic_with_null():
    oneof = {"oneOf": [{"type": "integer"}, {"type": "null"}]}
    expected = {"type": "INTEGER", "mode": "NULLABLE"}
    assert bq_schema(oneof) == expected


def test_incompatible_oneof_atomic():
    incompatible_multitype = {"oneOf": [{"type": "integer"}, {"type": "boolean"}]}
    expected = {"type": "STRING", "mode": "REQUIRED"}

    assert bq_schema(incompatible_multitype) == expected


def test_incompatible_oneof_atomic_with_null():
    """Test a oneOf clause and verify that the mode is NULLABLE.
    `null` has a logical-OR like behavior when there are choices of types.
    """

    incompatible_multitype = {
        "oneOf": [{"type": ["integer", "null"]}, {"type": "boolean"}]
    }
    expected = {"type": "STRING", "mode": "NULLABLE"}

    assert bq_schema(incompatible_multitype) == expected


def test_oneof_object_with_atomics():
    case = {
        "type": "object",
        "properties": {"field_1": {"type": "integer"}, "field_2": {"type": "integer"}},
    }
    oneof = {"oneOf": [case, case]}
    expected = {
        "type": "RECORD",
        "fields": [
            {"name": "field_1", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "field_2", "type": "INTEGER", "mode": "NULLABLE"},
        ],
        "mode": "REQUIRED",
    }

    assert bq_schema(oneof) == expected


def test_oneof_object_merge():
    """Test schemas that share common structure."""
    oneof = {
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "field_1": {"type": "integer"},
                    "field_3": {"type": "number"},
                },
            },
            {
                "type": "object",
                "properties": {
                    "field_2": {"type": "boolean"},
                    "field_3": {"type": "number"},
                },
            },
        ]
    }
    expected = {
        "type": "RECORD",
        "fields": [
            {"name": "field_1", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "field_2", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "field_3", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        "mode": "REQUIRED",
    }
    assert bq_schema(oneof) == expected


def test_oneof_object_merge_with_complex():
    oneof = {
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "namespace_1": {
                        "type": "object",
                        "properties": {
                            "field_1": {"type": "integer"},
                            "field_3": {"type": "number"},
                        },
                    }
                },
            },
            {
                "type": "object",
                "properties": {
                    "namespace_1": {
                        "type": "object",
                        "properties": {
                            "field_2": {"type": "boolean"},
                            "field_3": {"type": "number"},
                        },
                    }
                },
            },
            {
                "type": "object",
                "properties": {
                    "field_4": {"type": "boolean"},
                    "field_5": {"type": "number"},
                },
            },
        ]
    }
    expected = {
        "type": "RECORD",
        "fields": [
            {"name": "field_4", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "field_5", "type": "FLOAT", "mode": "NULLABLE"},
            {
                "name": "namespace_1",
                "type": "RECORD",
                "fields": [
                    {"name": "field_1", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "field_2", "type": "BOOLEAN", "mode": "NULLABLE"},
                    {"name": "field_3", "type": "FLOAT", "mode": "NULLABLE"},
                ],
                "mode": "NULLABLE",
            },
        ],
        "mode": "REQUIRED",
    }
    assert bq_schema(oneof) == expected


def test_incompatible_oneof_atomic_and_object():
    oneof = {
        "oneOf": [
            {"type": "integer"},
            {"type": "object", "properties": {"field_1": {"type": "integer"}}},
        ]
    }
    expected = {"type": "STRING", "mode": "REQUIRED"}

    assert bq_schema(oneof) == expected


def test_incompatible_oneof_object():
    oneof = {
        "oneOf": [
            {"type": "object", "properties": {"field_1": {"type": "integer"}}},
            {"type": "object", "properties": {"field_1": {"type": "boolean"}}},
        ]
    }
    expected = {"type": "STRING", "mode": "REQUIRED"}

    assert bq_schema(oneof) == expected


def test_incompatible_oneof_object_with_complex():
    """Test behavior of creating an incompatible leaf on a complex object.

    NOTE: A conflict at a node invalidates the entire tree. Another
    conflict resolution method is to treat diffs as json blobs while
    retaining as much structure as possible.
    """

    case_1 = {
        "type": "object",
        "properties": {
            "namespace_1": {
                "type": "object",
                "properties": {
                    "field_1": {"type": "string"},
                    "field_2": {"type": "integer"},
                },
            }
        },
    }
    # change a type at a leaf to render the tree incompatible
    case_2 = copy.deepcopy(case_1)
    case_2["properties"]["namespace_1"]["properties"]["field_1"]["type"] = "boolean"

    oneof = {"oneOf": [case_1, case_2]}
    # TODO: recursively handle typing conflicts
    expected = {"type": "STRING", "mode": "REQUIRED"}

    assert bq_schema(oneof) == expected
