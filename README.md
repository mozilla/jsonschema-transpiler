# jsonschema-transpiler

A program that translates JSON Schema into Avro or BigQuery schemas.
This allows the re-use of validation schemas to describe the structure of tables.


## Design considerations

JSONSchema is a language for describing the structure of JSON documents, primarily used to validating documents. 
A comprehensively written JSONSchema can be used to serialize documents into other binary formats, including Avro and Parquet.

### BigQuery
#### `NULLABLE` vs `REQUIRED`

The behavior between required, missing, and null fields can be ambiguous.
An atomic type like an integer is typically transformed into a required field.

```json
{"type": "integer"}
{"type": "INTEGER", "mode": "REQUIRED"}
```

A field that can be null is expressed using a list of atomic types:

```json
{"type": ["null", "integer"]}
{"type": "INTEGER", "mode": "NULLABLE"}
```

This behavior changes when the type is embedded as part of a complex type.
Should the mode of field `foo` in the BigQuery record be `REQUIRED` or `NULLABLE`?

```json
{"type": "object", "properties": {"foo": {"type": "integer"}}}
{"type": "RECORD", "mode": "REQUIRED", "fields": [{"name": "foo", "type": "INTEGER", "mode": "REQUIRED"}]}
```

Consider the case when all fields within an object are treated as `NULLABLE` unless the `required` keyword is defined.
This is technically correct behavior because an object property can be missing unless required.
We can also assume the opposite perspective and treat fields as `REQUIRED` unless type is a multi-type including `null`.
This is more explicit, but would be wrong in the case where the property was actually optional.


#### `oneOf` keyword

One of the features of JSONSchema is the ability to handle variant types through the use of the `oneOf` keyword.
For example, a particular field in a document could be treated as both an integer and an array of integers:

```json
{"oneOf": [{"type": "integer"}, {"type": "array", "items": {"type": "integer"}}]}
```

This would validate against the following JSON elements:

```json
42
[1, 1, 2, 3, 5]
```

The case provided is actually a degenerate one -- in most binary data formats, we do not have the ability to express a simple and complex element as a union.
We treat this sub-document as a string and let the consumer of the data handle further data processing.

However, this keyword makes sense when matching against objects that share common properties.
In this case, we would take the super-set of all of sub-schemas in order to find a common representation.
This process is done via a conflict-resolution procedure that determines whether there is a valid common representation.
