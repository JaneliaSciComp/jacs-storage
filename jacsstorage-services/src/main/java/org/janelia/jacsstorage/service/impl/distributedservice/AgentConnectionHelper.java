package org.janelia.jacsstorage.service.impl.distributedservice;

import java.util.List;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.model.support.JacsSubjectHelper;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.serviceutils.HttpClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AgentConnectionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AgentConnectionHelper.class);

    static StorageAgentInfo getAgentStatus(String agentUrl) {
        String agentStatusEndpoint = "/connection/status";
        Client httpClient = HttpClientUtils.createHttpClient();
        try {
            WebTarget target = httpClient.target(agentUrl).path(agentStatusEndpoint);
            Response response = target.request()
                    .get()
                    ;
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(StorageAgentInfo.class);
            } else {
                LOG.warn("Agent getStatus returned {}", response.getStatus());
                response.close();
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent getStatus", e);
        } finally {
            httpClient.close();
        }
        return null;
    }

    static List<UsageData> retrieveVolumeUsageData(String agentUrl, Number storageVolumeId, String subject) {
        Client httpClient = HttpClientUtils.createHttpClient();
        try {
            WebTarget target = httpClient.target(agentUrl)
                    .path("/agent_storage/volume_quota")
                    .path(storageVolumeId.toString())
                    .path("report");
            String subjectName = JacsSubjectHelper.getNameFromSubjectKey(subject);
            if (StringUtils.isNotBlank(subjectName)) {
                target = target.queryParam("subjectName", subjectName);
            }
            Invocation.Builder targetRequestBuilder = target.request()
                    .header("JacsSubject", subject)
                    ;
            Response response = targetRequestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                LOG.warn("Agent {} retrieve usage returned {} while trying to get usage data for {} on {}",
                        agentUrl, response.getStatus(), subject, storageVolumeId);
                response.close();
            } else {
                TypeReference<List<UsageData>> typeRef = new TypeReference<List<UsageData>>(){};
                return response.readEntity(new GenericType<>(typeRef.getType()));
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent retrieve usage for {} on {}", subject, storageVolumeId, e);
        } finally {
            httpClient.close();
        }
        return ImmutableList.of();
    }

    static List<UsageData> retrieveDataPathUsageData(String agentUrl, String storagePath, String subject) {
        Client httpClient = HttpClientUtils.createHttpClient();
        try {
            WebTarget target = httpClient.target(agentUrl)
                    .path("/agent_storage/path_quota")
                    .path(storagePath)
                    .path("report");
            String subjectName = JacsSubjectHelper.getNameFromSubjectKey(subject);
            if (StringUtils.isNotBlank(subjectName)) {
                target = target.queryParam("subjectName", subjectName);
            }
            Invocation.Builder targetRequestBuilder = target.request()
                    .header("JacsSubject", subject)
                    ;
            Response response = targetRequestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                LOG.warn("Agent {} retrieve usage returned {} while trying to get usage data for {} on {}",
                        agentUrl, response.getStatus(), subject, storagePath);
                response.close();
            } else {
                TypeReference<List<UsageData>> typeRef = new TypeReference<List<UsageData>>(){};
                return response.readEntity(new GenericType<>(typeRef.getType()));
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent retrieve usage for {} on {}", subject, storagePath, e);
        } finally {
            httpClient.close();
        }
        return ImmutableList.of();
    }

    static boolean deleteStorage(String agentUrl, Number dataBundleId, String subject, JacsCredentials jacsCredentials) {
        Client httpClient = HttpClientUtils.createHttpClient();
        Response response = null;
        try {
            WebTarget target = httpClient.target(agentUrl)
                    .path("agent_storage")
                    .path(dataBundleId.toString())
                    ;
            Invocation.Builder targetRequestBuilder = target.request()
                    .header("Authorization", getAuthorizationHeader(jacsCredentials))
                    .header("JacsSubject", subject)
                    ;
            response = targetRequestBuilder.delete();
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                LOG.warn("Agent {} delete storage returned {} while trying to delete {} by {}",
                        agentUrl, response.getStatus(), dataBundleId, subject);
            } else {
                return true;
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent delete storage", e);
        } finally {
            if (response != null) {
                response.close();
            }
            httpClient.close();
        }
        return false;
    }

    private static String getAuthorizationHeader(JacsCredentials jacsCredentials) {
        if (jacsCredentials == null) {
            return "";
        } else {
            return jacsCredentials.asAuthorizationHeader();
        }
    }
}
