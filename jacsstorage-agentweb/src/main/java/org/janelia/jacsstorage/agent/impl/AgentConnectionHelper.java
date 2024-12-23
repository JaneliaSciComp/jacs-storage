package org.janelia.jacsstorage.agent.impl;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.serviceutils.HttpClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(StorageAgentInfo.class);
            } else {
                LOG.warn("Agent deregistration returned {}", response.getStatus());
                response.close();
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error("Error raised while trying to find agent {} registration from {}", agentURL, masterServiceUrl, e);
            } else {
                LOG.error("Error raised while trying to find agent {} registration from {}: {}", agentURL, masterServiceUrl, e.toString());
            }
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
            } else {
                LOG.warn("Register agent with {} returned {}", target, responseStatus);
                response.close();
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error("Error while registering {}", agentInfo, e);
            } else {
                LOG.error("Error while registering {}: {}", agentInfo, e.toString());
            }
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
            response.close();
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error("Error raised while unregistering agent {} from {}", agentURL, masterServiceUrl, e);
            } else {
                LOG.error("Error raised while unregistering agent {} from {}: {}", agentURL, masterServiceUrl, e.toString());
            }
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }
}
