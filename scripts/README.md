# Scripts

This directory contains miscellaneous scripts that supplement the development of
this repository.

## Integration Scripts

The jsonschema-transpiler was developed for the GCP-ingestion pipeline that
ingests Firefox Telemetry directly into BigQuery. Because of the complexity
structure of the documents, the JSON payloads cannot be loaded directly into a
table. Instead, documents are first decoded into Avro using generated schemas to
guide serialization. This handles renaming columns and disambiguating structures
like objects from maps.

The mozilla-pipeline-schemas (mps) repo is the canonical source of schemas for
the Mozilla data platform. These JSON Schema are used by the ingestion pipeline
to validate incoming data for correctness. These scripts generate the necessary
avro schemas, downloads sampled pipeline data, and loads the data into a GCP
project. This allows manual inspection of the data.

You will need valid AWS credentials for accessing `s3://telemetry-parquet`, as
well as credentials for uploading to a new GCP project. Some scripts require
modification, so use at your own risk.

Run these scripts from the root of the project. 

```bash

# Check that the correct tools are installed
$ python3 --help
Python 3.7.2

$ gsutil --version
gsutil version: 4.37

$ bq version
This is BigQuery CLI 2.0.42

# Test AWS credentials
$ aws s3 ls s3://telemetry-parquet

# Install python dependencies
$ pip3 install --user avro-python3 python-rapidjson boto

# Generates a folder schemas/
$ ./scripts/mps-download-sampled-schemas.py

# Generates a folder data/
$ ./scripts/mps-download-sampled-data.py

# Generates a folder avro/
$ ./scripts/mps-generate-schemas.sh

# Alternatively, specify a folder and pass flags
$ ./scripts/mps-generate-schemas.sh \
    bq_schemas \
    --type bigquery \
    --resolve drop \
    --normalize-case

# Generates a folder avro-data/
$ ./scripts/mps-generate-avro-data.sh

# Uploads data to a GCP project: gcs://test-avro-ingest/data/
# Creates multiple BigQuery datasets and tables
$ ./scripts/load-avro-bq.sh
```