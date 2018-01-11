#!/usr/bin/env bash

source "${jacs.runtime.env.installDir}/etc/servicevars.sh"

cd "${jacs.runtime.env.installDir}"

JAVA_OPTS="${JAVA_OPTS}" \
bin/jacsstorage-masterweb \
     -b 0.0.0.0 \
     -p ${MASTER_PORT} \
     $*
