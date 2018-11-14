def test_allof_object():
    object_allof = {
        "allOf": [
            {
                "type": "object",
                "properties": {
                    "field_1": {"type": ["integer", "null"]},
                    "field_2": {"type": "string"},
                    "field_3": {"type": "boolean"},
                },
            },
            {"required": ["field_1", "field_3"]},
        ]
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

    assert bq_schema(object_allof) == expected
