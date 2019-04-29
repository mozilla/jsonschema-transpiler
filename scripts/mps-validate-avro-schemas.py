#!/usr/bin/env python3

"""A one off script to test whether mozilla-pipeline-schemas are generated
as valid avro schemas. This script requires avro to be installed, as well
as running the `mps-download-schemas.sh` script."""

import json
import logging
import os
import subprocess
import avro.schema


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BIN = "target/debug/jsonschema_transpiler"


def build():
    """Initialize a build of the transpiler."""
    subprocess.call(["cargo", "build"])


def avro_schema(path):
    """Return an avro schema in json if valid, None otherwise."""
    schema = None
    try:
        data = subprocess.check_output(
            [BIN, "--from-file", path, "--type", "avro"], stderr=subprocess.DEVNULL
        )
        schema = json.loads(data)
    except subprocess.CalledProcessError:
        pass
    return schema


def parse_schema_name(path):
    """Given a directory path to a json schema in the mps directory, generate
    the fully qualified name in the form `{namespace}.{doctype}.{docver}`."""
    elements = path.split("/")
    doctype, docver = elements[-1].split(".")[:-2]
    namespace = elements[-3]
    return f"{namespace}.{doctype}.{docver}"


def test_documents(mps_path):
    """Walk the schemas directory, generate the document, and parse it."""
    total = 0
    error = 0
    skipped = 0
    for root, _, files in os.walk(mps_path):
        for name in files:
            if name.endswith(".schema.json"):
                path = os.path.join(root, name)
                schema_name = parse_schema_name(path)
                schema = avro_schema(path)
                if not schema:
                    logging.info(f"failed to convert {schema_name}")
                    skipped += 1
                    continue
                total += 1
                try:
                    avro.schema.Parse(json.dumps(schema))
                except Exception as e:
                    logging.info(f"failed to parse {schema_name}")
                    error += 1
                    logging.debug(json.dumps(schema))
                    logging.exception(e)

    logging.info(f"{error}/{total} parsing errors, {skipped} skipped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    build()
    test_documents(os.path.join(ROOT, "schemas"))
