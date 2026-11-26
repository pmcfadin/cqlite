#!/bin/bash
# usage: bin_genjob.sh NAME "ARGS"
cd "$(dirname "$0")"
sed -e "s|@NAME@|$1|" -e "s|@ARGS@|$2|" manifests/gen-job.tmpl.yaml | ../bin/k apply -f -
