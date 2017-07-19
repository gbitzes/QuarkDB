#!/usr/bin/env bash

ROCKSDB_PATH=$(ci/canonical-rocksdb-path.sh)

if [[ "$?" != "0" ]]; then
  exit 0
fi

CONTENTS=$(ls -A "$ROCKSDB_PATH" &> /dev/null)
if [[ "$?" != "0" ]]; then
  exit 0
fi

printf "%s%s" "-DROCKSDB_ROOT_DIR=" "$ROCKSDB_PATH"
