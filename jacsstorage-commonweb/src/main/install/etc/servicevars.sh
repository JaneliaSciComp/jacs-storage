DEBUG_OPTS=""

JACSSTORAGE_CONFIG="${jacs.runtime.env.installDir}/etc/config.properties"
JACSSTORAGE_LOG_CONFIG="${jacs.runtime.env.installDir}/etc/logback.xml"

JAVA_OPTS="${DEBUG_OPTS} -Xmx2G -Xms512M -Dlogback.configurationFile=${JACSSTORAGE_LOG_CONFIG}"

AGENT_HOST=`hostname`
AGENT_HTTP_PORT=${jacs.runtime.env.agentHttpPort}

MASTER_HOST=${jacs.runtime.env.masterHost}
MASTER_HTTP_PORT=${jacs.runtime.env.masterHttpPort}
MASTER_URL="http://${MASTER_HOST}:${MASTER_HTTP_PORT}/jacsstorage/master_api/v1"
