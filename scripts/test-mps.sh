#!/bin/bash
# Test the jsonschema transpiler against documents in mozilla-pipeline-schemas.

cd "$(dirname "$0")/.." || exit

if [[ ! -d "schemas/" ]]; then
    echo "Run scripts/download-mps.sh to retrieve schemas"
    exit 1
fi

cargo build
bin="target/debug/jsonschema_transpiler"

schemas=$(find schemas/ -name "*.schema.json")

total=0
failed=0
for schema in $schemas; do
    if ! $bin -f "$schema" --type avro > /dev/null; then
        echo "Failed on $schema"
        ((failed++))
    fi
    ((total++))
done

echo "$failed/$total failures"