# jsonschema-transpiler

[![CircleCI](https://circleci.com/gh/mozilla/jsonschema-transpiler.svg?style=svg)](https://circleci.com/gh/mozilla/jsonschema-transpiler)

A tool for transpiling [JSON Schema](https://json-schema.org/) into schemas for
[Avro](https://avro.apache.org/docs/current/index.html#schemas) and
[BigQuery](https://cloud.google.com/bigquery/docs/schemas).

JSON Schema is primarily used to validate incoming data, but contains enough
information to describe the structure of the data. The transpiler encodes the
schema for use with data serialization and processing frameworks. The main
use-case is to enable ingestion of JSON documents into BigQuery through an Avro
intermediary.

This tool can handle many of the composite types seen in modern data processing
tools that support a SQL interface such as lists, structures, key-value
maps, and type-variants.

This tool is designed for generating new schemas from
[`mozilla-pipeline-schemas`](https://github.com/mozilla-services/mozilla-pipeline-schemas),
the canonical source of truth for JSON schemas in the Firefox Data Platform.

## Installation

```bash
cargo install jsonschema-transpiler
```

## Usage

```bash
A tool to transpile JSON Schema into schemas for data processing

USAGE:
    jsonschema-transpiler [FLAGS] [OPTIONS] [file]

FLAGS:
    -w, --allow-maps-without-value    Produces maps without a value field for incompatible or under-specified value
                                      schema
    -n, --force-nullable              Treats all columns as NULLABLE, ignoring the required section in the JSON Schema
                                      object
    -h, --help                        Prints help information
    -c, --normalize-case              snake_case column-names for consistent behavior between SQL engines
        --tuple-struct                Treats tuple validation as an anonymous struct
    -V, --version                     Prints version information

OPTIONS:
    -r, --resolve <resolve>    The resolution strategy for incompatible or under-specified schema [default: cast]
                               [possible values: cast, panic, drop]
    -t, --type <type>          The output schema format [default: avro]  [possible values: avro, bigquery]

ARGS:
    <file>    Sets the input file to use
```

JSON Schemas can be read from stdin or from a file.

### Examples usage

```bash
# An object with a single, optional boolean field
$ schema='{"type": "object", "properties": {"foo": {"type": "boolean"}}}'

$ echo $schema | jq
{
  "type": "object",
  "properties": {
    "foo": {
      "type": "boolean"
    }
  }
}

$ echo $schema | jsonschema-transpiler --type avro
{
  "fields": [
    {
      "name": "foo",
      "type": [
        {
          "type": "null"
        },
        {
          "type": "boolean"
        }
      ]
    }
  ],
  "name": "root",
  "type": "record"
}

$ echo $schema | jsonschema-transpiler --type bigquery
{
  "fields": [
    {
      "mode": "NULLABLE",
      "name": "foo",
      "type": "BOOL"
    }
  ],
  "mode": "REQUIRED",
  "type": "RECORD"
}
```

## Building

To build and test the package:

```bash
cargo build
cargo test
```

Older versions of the package (<= 1.9) relied on the use of oniguruma for
performing snake-casing logic. To enable the use of this module, add a feature
flag:

```bash
cargo test --features oniguruma
```

## Contributing

Contributions are welcome. The API may change significantly, but the
transformation between various source formats should remain consistent. To aid
in the development of the transpiler, tests cases are generated from a language
agnostic format under `tests/resources`.

```json
{
    "name": "test-suite",
    "tests": [
        {
            "name": "test-case",
            "description": [
                "A short description of the test case."
            ],
            "tests": {
                "avro": {...},
                "bigquery": {...},
                "json": {...}
            }
        },
        ...
    ]
}
```

Schemas provide a type system for data-structures. Most schema languages support
a similar set of primitives. There are atomic data types like booleans,
integers, and floats. These atomic data types can form compound units of
structure, such as objects, arrays, and maps. The absence of a value is usually
denoted by a null type. There are type modifiers, like the union of two types.

The following schemas are currently supported:

- JSON Schema
- Avro
- BigQuery

In the future, it may be possible to support schemas from similar systems like
Parquet and Spark, or into various interactive data languages (IDL) like
Avro IDL.

## Publishing

The jsonschema-transpiler is distributed as a crate via Cargo. Follow this
checklist for deploying to [crates.io](https://crates.io/crates/jsonschema-transpiler).

1. Bump the version number in the `Cargo.toml`, as per [Semantic Versioning](https://semver.org/).
2. Double check that `cargo test` and CI succeeds.
3. Run `cargo publish`. It must be run with the `--no-verify` flag due to issue #59.
4. Draft a new release in GitHub corresponding with the version bump.
