#!/bin/bash

cd "$(dirname "$0")/.." || exit

documents=$(ls data | sed 's/.ndjson//')

total=0
failed=0
for document in $documents; do
    if ! python3 scripts/mps-generate-avro-data-helper.py $document; then
        echo "failed to write $document"
        rm "avro-data/$document.avro"
        ((failed++))
    fi
    ((total++))
done

echo "$((total - failed))/$total succeeded"