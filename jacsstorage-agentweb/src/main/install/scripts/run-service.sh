#!/usr/bin/env bash

source "${jacs.runtime.env.installDir}/etc/servicevars.sh"

cd "${jacs.runtime.env.installDir}"

JAVA_OPTS="${JAVA_OPTS}" \
bin/jacsstorage-agentweb \
     -b 0.0.0.0 \
     -p ${AGENT_HTTP_PORT} \
     -tcpBind 0.0.0.0 \
     -tcpPort ${AGENT_TCP_PORT} \
     -masterURL ${MASTER_URL} \
     -DStorageAgent.InitialPingDelayInSeconds=0 \
     $*
