#!/bin/bash
# Test the jsonschema transpiler against documents in mozilla-pipeline-schemas.

cd "$(dirname "$0")/.." || exit

if [[ ! -d "schemas/" ]]; then
    echo "Run scripts/mps-download-schemas.sh to retrieve schemas"
    exit 1
fi

cargo build
bin="target/debug/jsonschema-transpiler"

schemas=$(find schemas/ -name "*.schema.json")

# create a new folder for avro schemas
outdir=${1:-"avro"}
if [[ -d $outdir ]]; then
    rm -r $outdir
fi
shift;

mkdir $outdir

total=0
failed=0
for schema in $schemas; do
    namespace=$(basename $(dirname $(dirname $schema)))
    schema_filename=$(basename $schema)
    outfile="$outdir/$namespace.$schema_filename"

    if ! $bin "$@" "$schema" > $outfile; then
        echo "Failed on $schema"
        rm $outfile
        ((failed++))
    fi
    ((total++))
done

echo "$((total - failed))/$total succeeded"
