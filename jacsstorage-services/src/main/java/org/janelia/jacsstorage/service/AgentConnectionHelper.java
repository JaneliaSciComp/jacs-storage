package org.janelia.jacsstorage.service;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

class AgentConnectionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AgentConnectionHelper.class);

    static StorageAgentInfo getAgentStatus(String agentUrl) {
        String agentStatusEndpoint = String.format("/connection/status");
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
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

    static boolean cleanupStorage(String agentUrl, String dataPath) {
        String deleteStorageEndpoint = String.format("/agent-storage/absolute-path-to-clean/%s", dataPath);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(agentUrl)
                    .path(deleteStorageEndpoint);
            Response response = target.request()
                    .delete()
                    ;
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                LOG.warn("Agent storage cleanup returned {}", response.getStatus());
            } else {
                return true;
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent storage cleanup", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return false;
    }

    static boolean deleteStorage(String agentUrl, String dataPath, String parentPath) {
        String deleteStorageEndpoint = String.format("/agent-storage/absolute-path/%s", dataPath);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(agentUrl)
                        .path(deleteStorageEndpoint);
            if (StringUtils.isNotBlank(parentPath)) {
                target = target.queryParam("parent-path", parentPath);
            }
            Response response = target.request()
                    .delete()
                    ;
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                LOG.warn("Agent delete storage returned {}", response.getStatus());
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

    static InputStream streamDataFromStorage(String agentUrl, JacsStorageFormat storageFormat, String dataPath) {
        String retrieveStreamEndpoint = String.format("/agent-storage/format/%s/absolute-path/%s", storageFormat, dataPath);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(agentUrl).path(retrieveStreamEndpoint);
            Response response = target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .get()
                    ;
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                LOG.warn("Agent getStatus returned {}", response.getStatus());
            } else {
                return response.readEntity(InputStream.class);
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

    static TransferInfo streamDataToStorage(String agentUrl, JacsStorageFormat storageFormat, String dataPath, InputStream dataStream) {
        String persistStreamEndpoint = String.format("/agent-storage/format/%s/absolute-path/%s", storageFormat, dataPath);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(agentUrl).path(persistStreamEndpoint);
            Response response = target.request()
                    .post(Entity.entity(dataStream, MediaType.APPLICATION_OCTET_STREAM_TYPE))
                    ;
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                LOG.warn("Agent stream data returned {}", response.getStatus());
            } else {
                return response.readEntity(TransferInfo.class);
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent stream data", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return null;
    }

    private static Client createHttpClient() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLSv1");
        TrustManager[] trustManagers = {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String authType) throws CertificateException {
                        // Everyone is trusted
                    }
                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String authType) throws CertificateException {
                        // Everyone is trusted
                    }
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        sslContext.init(null, trustManagers, new SecureRandom());
        return ClientBuilder.newBuilder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .sslContext(sslContext)
                .hostnameVerifier((s, sslSession) -> true)
                .register(new JacksonFeature())
                .build();
    }

}
