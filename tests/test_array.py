def test_array_with_atomics():
    array_atomic = {"type": "array", "items": {"type": "integer"}}
    expected = {"type": "INTEGER", "mode": "REPEATED"}

    assert bq_schema(array_atomic) == expected


def test_array_with_complex():
    array_complex = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "field_1": {"type": "string"},
                "field_2": {"type": "integer"},
            },
        },
    }
    expected = {
        "mode": "REPEATED",
        "type": "RECORD",
        "fields": [
            {"name": "field_1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "field_2", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    }

    assert bq_schema(array_complex) == expected
