package org.janelia.jacsstorage.agent;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.serviceutils.HttpClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

class AgentConnectionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AgentConnectionHelper.class);

    static StorageAgentInfo findRegisteredAgent(String masterServiceUrl, String agentURL) {
        String registrationEndpoint = "/agents/url";
        Client httpClient = null;
        try {
            httpClient = HttpClientUtils.createHttpClient();
            WebTarget target = httpClient.target(masterServiceUrl)
                    .path(registrationEndpoint)
                    .path(agentURL);
            Response response = target.request()
                    .get()
                    ;
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                LOG.warn("Agent deregistration returned {}", response.getStatus());
            } else {
                return response.readEntity(StorageAgentInfo.class);
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent deregistration", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return null;
    }

    static StorageAgentInfo registerAgent(String masterServiceUrl, StorageAgentInfo agentInfo) {
        String registrationEndpoint = "/agents";
        Client httpClient = null;
        try {
            httpClient = HttpClientUtils.createHttpClient();
            WebTarget target = httpClient.target(masterServiceUrl).path(registrationEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .post(Entity.json(agentInfo))
                    ;

            int responseStatus = response.getStatus();
            if (responseStatus < Response.Status.BAD_REQUEST.getStatusCode()) {
                return response.readEntity(StorageAgentInfo.class);
            }
            LOG.warn("Register agent returned {}", responseStatus);
        } catch (Exception e) {
            LOG.error("Error while registering {}", agentInfo, e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return null;
    }

    static void deregisterAgent(String masterServiceUrl, String agentURL, String agentToken) {
        String registrationEndpoint = "/agents/url";
        Client httpClient = null;
        try {
            httpClient = HttpClientUtils.createHttpClient();
            WebTarget target = httpClient.target(masterServiceUrl)
                    .path(registrationEndpoint)
                    .path(agentURL)
                    ;
            Response response = target.request()
                    .header("agentToken", agentToken)
                    .delete()
                    ;
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                LOG.warn("Agent deregistration returned {}", response.getStatus());
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent deregistration", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

}
