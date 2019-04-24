#!/usr/bin/env bash

source "${jacs.runtime.env.installDir}/etc/servicevars.sh"

cd "${jacs.runtime.env.installDir}"

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"
EXPOSED_AGENT_HOST=`hostname`

JAVA_OPTS="${JAVA_OPTS}" JACSSTORAGE_CONFIG="${JACSSTORAGE_CONFIG}" \
bin/jacsstorage-agentweb \
     -b 0.0.0.0 \
     -p ${AGENT_HTTP_PORT} \
     -masterURL ${MASTER_URL} \
     -bootstrapStorageVolumes \
     -DStorageAgent.InitialPingDelayInSeconds=0 \
     -DStorageAgent.StorageHost=${EXPOSED_AGENT_HOST} \
     $*
