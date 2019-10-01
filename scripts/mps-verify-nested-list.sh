#!/bin/bash

cd "$(dirname "$0").."

datadir=$(mktemp -d -t tmp.XXXXXXXXXX)
function cleanup {
    echo "Running cleanup!"
    rm -rf "$datadir"
}
trap cleanup EXIT

scripts/mps-download-schemas.sh

avro_no_tuple_control=$datadir/avro-no-tuple-control
avro_tuple_control=$datadir/avro-tuple-control
avro_no_tuple=$datadir/avro-no-tuple
avro_tuple=$datadir/avro-tuple

bq_no_tuple_control=$datadir/bq-no-tuple-control
bq_tuple_control=$datadir/bq-tuple-control
bq_no_tuple=$datadir/bq-no-tuple
bq_tuple=$datadir/bq-tuple


# get control values
git checkout v1.5.0

scripts/mps-generate-schemas.sh $avro_no_tuple_control --type avro --resolve drop
scripts/mps-generate-schemas.sh $avro_tuple_control --type avro --resolve drop --tuple-struct
scripts/mps-generate-schemas.sh $bq_no_tuple_control --type bigquery --resolve drop
scripts/mps-generate-schemas.sh $bq_tuple_control --type bigquery --resolve drop --tuple-struct

git checkout -

# get values for tuple/no-tuple
scripts/mps-generate-schemas.sh $avro_no_tuple --type avro --resolve drop
scripts/mps-generate-schemas.sh $avro_tuple --type avro --resolve drop --tuple-struct
scripts/mps-generate-schemas.sh $bq_no_tuple --type bigquery --resolve drop
scripts/mps-generate-schemas.sh $bq_tuple --type bigquery --resolve drop --tuple-struct

outdir="test_nested_list_results"
mkdir -p $outdir

diff -r $avro_no_tuple_control $avro_no_tuple > $outdir/avro-no-tuple.diff
diff -r $avro_tuple_control $avro_tuple > $outdir/avro-tuple.diff
diff -r $bq_no_tuple_control $bq_no_tuple > $outdir/bq-no-tuple.diff
diff -r $bq_tuple_control $bq_tuple > $outdir/bq-tuple.diff
