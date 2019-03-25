#!/bin/bash

cd "$(dirname "$0")/.." || exit

project_id="test-avro-ingest"

gsutil -m cp avro-data/* gs://${project_id}/data

documents=$(ls avro-data | sed 's/.avro//')

total=0
error=0
skip=0

trap "exit" INT
for document in ${documents}; do
    # downcase hyphens to underscores before generating names
    bq_document=$(echo $document | sed 's/-/_/g')
    namespace=$(echo $bq_document | cut -d. -f1)
    doctype=$(echo $bq_document | cut -d. -f2)
    docver=$(echo $bq_document | cut -d. -f3)

    if ! bq ls | grep ${namespace} >/dev/null ; then
        echo "creating dataset: ${namespace}"
        bq mk ${namespace}
    fi

    table_exists=$(bq ls ${namespace} | grep ${doctype}_v${docver})

    if [[ ! -z ${SKIP_EXISTING+x} ]] && [[ ! -z ${table_exists} ]]; then
        echo "skipping bq load for ${document}"
        ((skip++))
        continue
    fi

    echo "running bq load for ${document}"
    bq load --source_format=AVRO \
        --replace \
        ${namespace}.${doctype}_v${docver} \
        gs://${project_id}/data/${document}.avro
    
    if [[ $? -ne 0 ]]; then
        ((error++))
    fi
    ((total++))
done

echo "$((total-error))/$total loaded successfully, $skip skipped"