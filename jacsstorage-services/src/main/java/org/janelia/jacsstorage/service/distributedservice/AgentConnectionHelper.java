package org.janelia.jacsstorage.service.distributedservice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.security.JacsSubjectHelper;
import org.janelia.jacsstorage.serviceutils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.List;

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

    static List<UsageData> retrieveVolumeUsageData(String agentUrl, Number storageVolumeId, String subject, String authToken) {
        String storageUsageEndpoint = String.format("/agent_storage/quota/%s/report/%s",
                storageVolumeId,
                StringUtils.defaultIfBlank(JacsSubjectHelper.getNameFromSubjectKey(subject), ""));
        Client httpClient = null;
        try {
            httpClient = HttpUtils.createHttpClient();
            WebTarget target = httpClient.target(agentUrl)
                    .path(storageUsageEndpoint);
            Invocation.Builder targetRequestBuilder = target.request()
                    .header("Authorization", "Bearer " + authToken)
                    .header("JacsSubject", subject)
                    ;
            Response response = targetRequestBuilder.delete();
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                LOG.warn("Agent {} retrieve usage returned {} while trying to get usage data for {} on {}",
                        agentUrl, response.getStatus(), subject, storageVolumeId);
            } else {
                TypeReference<List<UsageData>> typeRef = new TypeReference<List<UsageData>>(){};
                return response.readEntity(new GenericType<>(typeRef.getType()));
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent retrieve usage for {} on {}", subject, storageVolumeId, e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return ImmutableList.of();
    }

    static List<UsageData> retrieveDataPathUsageData(String agentUrl, String storagePath, String subject, String authToken) {
        String storageUsageEndpoint = String.format("/agent_storage/path_quota/%s/report/%s",
                storagePath,
                StringUtils.defaultIfBlank(JacsSubjectHelper.getNameFromSubjectKey(subject), ""));
        Client httpClient = null;
        try {
            httpClient = HttpUtils.createHttpClient();
            WebTarget target = httpClient.target(agentUrl)
                    .path(storageUsageEndpoint);
            Invocation.Builder targetRequestBuilder = target.request()
                    .header("Authorization", "Bearer " + authToken)
                    .header("JacsSubject", subject)
                    ;
            Response response = targetRequestBuilder.delete();
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                LOG.warn("Agent {} retrieve usage returned {} while trying to get usage data for {} on {}",
                        agentUrl, response.getStatus(), subject, storagePath);
            } else {
                TypeReference<List<UsageData>> typeRef = new TypeReference<List<UsageData>>(){};
                return response.readEntity(new GenericType<>(typeRef.getType()));
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent retrieve usage for {} on {}", subject, storagePath, e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return ImmutableList.of();
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
