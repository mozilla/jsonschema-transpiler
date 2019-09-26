#!/bin/bash
# Download production mozilla-pipeline-schemas into a schema folder

cd "$(dirname "$0")/.." || exit
curl -o schemas.tar.gz -L https://github.com/mozilla-services/mozilla-pipeline-schemas/archive/master.tar.gz
tar --strip-components=1 -xvf schemas.tar.gz mozilla-pipeline-schemas-master/schemas
rm schemas.tar.gz
