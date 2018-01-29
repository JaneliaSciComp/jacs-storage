#!/usr/bin/env bash

source "${jacs.runtime.env.installDir}/etc/servicevars.sh"

cd "${jacs.runtime.env.installDir}"

JAVA_OPTS="${JAVA_OPTS}" JACSSTORAGE_CONFIG="${JACSSTORAGE_CONFIG}" \
bin/jacsstorage-agentweb \
     -b 0.0.0.0 \
     -p ${AGENT_HTTP_PORT} \
     -masterURL ${MASTER_URL} \
     -DStorageAgent.InitialPingDelayInSeconds=0 \
     $*
