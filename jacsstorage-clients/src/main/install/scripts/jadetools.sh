#!/usr/bin/env bash

DEBUG_OPTS=""

JAVA_OPTS="${DEBUG_OPTS} -Xmx2G -Xms512M"

MASTER_HOST=${jacs.runtime.env.masterHost}
MASTER_HTTP_PORT=${jacs.runtime.env.masterHttpPort}
MASTER_URL="http://${MASTER_HOST}:${MASTER_HTTP_PORT}/jacsstorage/master_api/v1"

cd "${jacs.runtime.env.installDir}"

JAVA_OPTS="${JAVA_OPTS}" JACSSTORAGE_CONFIG="${JACSSTORAGE_CONFIG}" \
bin/storageClientApp -server "${MASTER_URL}" $*