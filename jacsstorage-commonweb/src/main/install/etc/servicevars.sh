JACSSTORAGE_CONFIG="${jacs.runtime.env.installDir}/etc/config.properties"
JACSSTORAGE_LOG_CONFIG="${jacs.runtime.env.installDir}/etc/logback.xml"

DEBUG_OPTS=""
JAVA_OPTS="${DEBUG_OPTS} -Xmx2G -Xms512M -Dlogback.configurationFile=${JACSSTORAGE_LOG_CONFIG}"

AGENT_HOST=`hostname -s`
AGENT_HTTP_PORT=9881
AGENT_TCP_PORT=11000

MASTER_IP=jade1
MASTER_PORT=9880
MASTER_URL="http://${MASTER_IP}:${MASTER_PORT}/jacsstorage/master_api/v1"
