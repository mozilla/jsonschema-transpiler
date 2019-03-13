#/bin/bash
# Test the jsonschema transpiler against documents in mozilla-pipeline-schemas.

cd "$(dirname "$0")/.."

if [[ ! -d "schemas/" ]]; then
    echo "Run scripts/download-mps.sh to retrieve schemas"
    exit 1
fi

cargo install --path . --force

schemas=$(find schemas/ -name "*.schema.json")

total=0
failed=0
for schema in $schemas; do
    jsonschema_transpiler -f $schema --type avro > /dev/null
    if [[ $? != 0 ]]; then
        echo "Failed on $schema"
        let failed++
    fi
    let total++
done

echo "$failed/$total failures"