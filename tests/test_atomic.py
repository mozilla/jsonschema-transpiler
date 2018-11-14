"""Check that the base case of the schema is being handled properly."""


def test_atomic():
    atomic = {'type': 'integer'}
    expected = {'type': 'INTEGER', 'mode': 'REQUIRED'}

    assert bq_schema(atomic) == expected


def test_atomic_with_null():
    atomic = {'type': ['integer', 'null']}
    expected = {'type': 'INTEGER', 'mode': 'NULLABLE'}

    assert bq_schema(atomic) == expected


def test_incompatible_atomic_multitype():
    """Test overlapping types are treated as json blobs."""

    atomic = {'type': ['boolean', 'integer']}
    expected = {'type': 'STRING', 'mode': 'REQUIRED'}

    assert bq_schema(atomic) == expected


def test_incompatible_atomic_multitype_with_null():
    """Test overlapping types that can be null are nullable json blobs.
    A field is null if any of it's types are null"""

    atomic = {'type': ['boolean', 'integer', 'null']}
    expected = {'type': 'STRING', 'mode': 'NULLABLE'}

    assert bq_schema(atomic) == expected