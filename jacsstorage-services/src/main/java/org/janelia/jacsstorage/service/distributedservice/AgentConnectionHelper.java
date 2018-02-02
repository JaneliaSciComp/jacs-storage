package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.serviceutils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

class AgentConnectionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AgentConnectionHelper.class);

    static StorageAgentInfo getAgentStatus(String agentUrl) {
        String agentStatusEndpoint = "/connection/status";
        Client httpClient = null;
        try {
            httpClient = HttpUtils.createHttpClient();
            WebTarget target = httpClient.target(agentUrl).path(agentStatusEndpoint);
            Response response = target.request()
                    .get()
                    ;
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                LOG.warn("Agent getStatus returned {}", response.getStatus());
            } else {
                return response.readEntity(StorageAgentInfo.class);
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent getStatus", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return null;
    }

    static boolean deleteStorage(String agentUrl, Number dataBundleId, String subject, String authToken) {
        String deleteStorageEndpoint = String.format("/agent_storage/%d", dataBundleId);
        Client httpClient = null;
        try {
            httpClient = HttpUtils.createHttpClient();
            WebTarget target = httpClient.target(agentUrl)
                        .path(deleteStorageEndpoint);
            Invocation.Builder targetRequestBuilder = target.request()
                    .header("Authorization", "Bearer " + authToken)
                    .header("JacsSubject", subject)
                    ;
            Response response = targetRequestBuilder.delete();
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                LOG.warn("Agent {} delete storage returned {} while trying to delete {} by {}",
                        agentUrl, response.getStatus(), dataBundleId, subject);
            } else {
                return true;
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent delete storage", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return false;
    }

}
