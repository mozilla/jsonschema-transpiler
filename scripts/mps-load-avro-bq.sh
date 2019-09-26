#!/bin/bash

cd "$(dirname "$0")/.." || exit

project_id=$(gcloud config get-value project)
dataset_id="test_avro"

gsutil -m cp avro-data/* gs://${project_id}/data
bq rm -rf $dataset_id
bq mk $dataset_id

total=0
error=0
skip=0

trap "exit" INT
for document in $(ls avro-data | sed 's/.avro//'); do
    # downcase hyphens to underscores before generating names
    bq_document=$(echo $document | sed 's/-/_/g')
    namespace=$(echo $bq_document | cut -d. -f1)
    doctype=$(echo $bq_document | cut -d. -f2)
    docver=$(echo $bq_document | cut -d. -f3)

    table_exists=$(bq ls ${dataset_id} | grep ${namespace}__${doctype}_v${docver})

    if [[ ! -z ${SKIP_EXISTING+x} ]] && [[ ! -z ${table_exists} ]]; then
        echo "skipping bq load for ${document}"
        ((skip++))
        continue
    fi

    echo "running bq load for ${document}"
    bq load --source_format=AVRO \
        --replace \
        ${dataset_id}.${namespace}__${doctype}_v${docver} \
        gs://${project_id}/data/${document}.avro
    
    if [[ $? -ne 0 ]]; then
        ((error++))
    fi
    ((total++))
done

echo "$((total-error))/$total loaded successfully, $skip skipped"