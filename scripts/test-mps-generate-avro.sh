#!/bin/bash

cd "$(dirname "$0")/.." || exit

documents=$(ls data | sed 's/.ndjson//')

total=0
failed=0
for document in $documents; do
    if ! python3 scripts/generate-avro.py $document 2> /dev/null; then
        echo "failed to write $document"
        rm "avro-data/$document.avro"
        ((failed++))
    fi
    ((total++))
done

echo "$((total - failed))/$total succeeded"