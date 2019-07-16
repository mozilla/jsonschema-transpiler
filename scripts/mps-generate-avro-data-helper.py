#!/usr/bin/env python3
import io
import json
import os
import sys

import avro.datafile
import avro.io
import avro.schema
from fastavro import parse_schema, validation

if len(sys.argv) > 1:
    # formatted as {namespace}.{doctype}.{docver}
    document = sys.argv[1]
else:
    sys.exit("Error: missing argument for document")

assert any(
    [document in name for name in os.listdir("data")]
), "document not found in data"
assert any(
    [document in name for name in os.listdir("avro")]
), "document not found in avro schemas"


def format_key(key):
    if not key:
        raise ValueError("empty key not allowed")
    key = key.replace("-", "_").replace(".", "_")
    if key[0].isdigit():
        key = "_" + key
    return key


def convert(data, schema):

    if schema.type == "string":
        if not isinstance(data, str):
            return json.dumps(data)

    if schema.type == "record":
        # iterate over all keys
        out = {}
        if not data:
            return out
        for key, value in data.items():
            # apply the appropriate transformations on the key
            key = format_key(key)
            field = schema.field_map.get(key)
            if not field:
                continue
            out[key] = convert(value, field.type)
        return out

    if schema.type == "union":
        for sub in schema.schemas:
            if sub.type == "null":
                continue
            out = convert(data, sub)
        return out

    if schema.type == "array":
        out = []
        if not data:
            return out
        for item in data:
            out.append(convert(item, schema.items))
        return out

    if schema.type == "map":
        out = {}
        for key, value in data.items():
            out[key] = convert(value, schema.values)
        return out

    # terminal node, do nothing
    return data


outdir = "avro-data"
if not os.path.exists(outdir):
    os.makedirs(outdir)

with open(f"avro/{document}.schema.json", "r") as f:
    schema_data = f.read()
schema = avro.schema.Parse(schema_data)

outfile = open(f"{outdir}/{document}.avro", "wb")
writer = avro.datafile.DataFileWriter(outfile, avro.io.DatumWriter(), schema)

with open(f"data/{document}.ndjson", "r") as f:
    data = f.readlines()

try:
    out = {}
    for line in data:
        out = convert(json.loads(line), schema)
        writer.append(out)
except:
    with open("test.json", "w") as f:
        json.dump(out, f)
    with open("test-schema.json", "w") as f:
        json.dump(schema.to_json(), f, indent=2)
    validation.validate(out, parse_schema(schema.to_json()))

writer.close()
