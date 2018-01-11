DEBUG_OPTS=""

JACSSTORAGE_CONFIG="${jacs.runtime.env.installDir}/etc/config.properties"
JACSSTORAGE_LOG_CONFIG="${jacs.runtime.env.installDir}/etc/logback.xml"

JAVA_OPTS="${DEBUG_OPTS} -Xmx2G -Xms512M -Dlogback.configurationFile=${JACSSTORAGE_LOG_CONFIG}"

AGENT_HOST=`hostname -s`
AGENT_HTTP_PORT=${jacs.runtime.env.agentHttpPort}
AGENT_TCP_PORT=${jacs.runtime.env.agentTcpPort}

MASTER_IP=jade1
MASTER_PORT=${jacs.runtime.env.masterHttpPort}
MASTER_URL="http://${MASTER_IP}:${MASTER_PORT}/jacsstorage/master_api/v1"
