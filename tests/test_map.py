"""Test the behavior of repeated key-value structures.
This is influenced strongly by the data-structures used in collecting
metrics. They have different names but common structure.
This type of output structure can be handled efficiently with the use of
`UNNEST` and projections.
An alternative is to dump the entire structure to JSON and use javascript
UDFs to handle processing.
"""

def test_map_with_atomics():
    map_atomic = {
        "type": "object",
        "additionalProperties": {"type": "integer"}
    }
    expected = {
        'mode': 'REPEATED',
        'type': 'RECORD',
        'fields': [
            {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'value', 'type': 'INTEGER', 'mode': 'REQUIRED'}
        ]
    }
    assert bq_schema(map_atomic) == expected


def test_map_with_complex():
    map_complex = {
        "type": "object",
        "additionalProperties": {
            "type": "object",
            "properties": {
                "field_1": {"type": "string"},
                "field_2": {"type": "integer"}
            }
        }
    }
    expected = {
        'mode': 'REPEATED',
        'type': 'RECORD',
        'fields': [
            {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {
                'name': 'value',
                'type': 'RECORD',
                'fields': [
                    {'name': 'field_1', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'field_2', 'type': 'INTEGER', 'mode': 'NULLABLE'}
                ],
                'mode': 'REQUIRED'
            }
        ]
    }
    assert bq_schema(map_complex) == expected


def test_map_with_pattern_properties():
    map_complex = {
        "type": "object",
        "patternProperties": {
            ".+": {"type": "integer"}
        },
        "additionalProperties": False
    }
    expected = {
        'mode': 'REPEATED',
        'type': 'RECORD',
        'fields': [
            {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'value', 'type': 'INTEGER', 'mode': 'REQUIRED'}
        ]
    }

    assert bq_schema(map_complex) == expected


def test_map_with_pattern_and_additional_properties():
    map_complex = {
        "type": "object",
        "patternProperties": {
            ".+": {"type": "integer"}
        },
        "additionalProperties": {"type": "integer"}
    }
    expected = {
        'mode': 'REPEATED',
        'type': 'RECORD',
        'fields': [
            {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'value', 'type': 'INTEGER', 'mode': 'REQUIRED'}
        ]
    }

    assert bq_schema(map_complex) == expected


def test_incompatible_map_with_pattern_properties():
    incompatible_map = {
        "type": "object",
        "patternProperties": {
            "^S_": {"type": "string"},
            "^I_": {"type": "integer"}
        },
        "additionalProperties": False
    }
    expected = {
        'mode': 'REPEATED',
        'type': 'RECORD',
        'fields': [
            {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'value', 'type': 'STRING', 'mode': 'REQUIRED'}
        ]
    }
    assert bq_schema(incompatible_map) == expected


def test_incompatible_map_with_pattern_and_additional_properties():
    incompatible_map = {
        "type": "object",
        "patternProperties": {
            ".+": {"type": "string"}
        },
        "additionalProperties": {"type": "integer"}
    }
    expected = {
        'mode': 'REPEATED',
        'type': 'RECORD',
        'fields': [
            {'name': 'key', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'value', 'type': 'STRING', 'mode': 'REQUIRED'}
        ]
    }
    assert bq_schema(incompatible_map) == expected