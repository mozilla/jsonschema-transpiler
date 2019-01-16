def test_object_with_atomics_is_sorted():
    """Test that fields are sorted in a record.
    Sorting makes the output schema deterministic.
    """

    object_atomic = {
        "type": "object",
        "properties": {
            "field_1": {"type": "integer"},
            "field_4": {"type": "number"},
            "field_3": {"type": "boolean"},
            "field_2": {"type": "string"},
        },
    }
    expected = {
        "type": "RECORD",
        "fields": [
            {"name": "field_1", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "field_2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "field_3", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "field_4", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        "mode": "REQUIRED",
    }

    assert bq_schema(object_atomic) == expected


def test_object_with_atomics_required():
    """Test that required fields have the required mode.
    This changes the mode of the underlying atomic field.
    """
    object_atomic = {
        "type": "object",
        "properties": {
            "field_1": {"type": "integer"},
            "field_2": {"type": "string"},
            "field_3": {"type": "boolean"},
        },
        "required": ["field_1", "field_3"],
    }
    expected = {
        "type": "RECORD",
        "fields": [
            {"name": "field_1", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "field_2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "field_3", "type": "BOOLEAN", "mode": "REQUIRED"},
        ],
        "mode": "REQUIRED",
    }

    assert bq_schema(object_atomic) == expected


def test_object_with_atomics_required_with_null():
    """Test the output of a nullable required field.
    The field is casted from nullable to required at the object level.
    Since the underlying field is null, the field is then casted back
    to nullable.
    """
    object_atomic = {
        "type": "object",
        "properties": {
            "field_1": {"type": ["integer", "null"]},
            "field_2": {"type": "string"},
            "field_3": {"type": "boolean"},
        },
        "required": ["field_1", "field_3"],
    }
    expected = {
        "type": "RECORD",
        "fields": [
            {"name": "field_1", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "field_2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "field_3", "type": "BOOLEAN", "mode": "REQUIRED"},
        ],
        "mode": "REQUIRED",
    }

    assert bq_schema(object_atomic) == expected


def test_object_with_complex():
    object_complex = {
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
    expected = {
        "type": "RECORD",
        "fields": [
            {
                "name": "namespace_1",
                "type": "RECORD",
                "fields": [
                    {"name": "field_1", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "field_2", "type": "INTEGER", "mode": "NULLABLE"},
                ],
                "mode": "NULLABLE",
            }
        ],
        "mode": "REQUIRED",
    }

    assert bq_schema(object_complex) == expected
