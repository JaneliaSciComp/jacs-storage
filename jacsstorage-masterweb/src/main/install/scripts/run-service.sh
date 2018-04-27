#!/usr/bin/env bash

source "${jacs.runtime.env.installDir}/etc/servicevars.sh"

cd "${jacs.runtime.env.installDir}"

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

JAVA_OPTS="${JAVA_OPTS}" JACSSTORAGE_CONFIG="${JACSSTORAGE_CONFIG}" \
bin/jacsstorage-masterweb \
     -b 0.0.0.0 \
     -p ${MASTER_HTTP_PORT} \
     $*
