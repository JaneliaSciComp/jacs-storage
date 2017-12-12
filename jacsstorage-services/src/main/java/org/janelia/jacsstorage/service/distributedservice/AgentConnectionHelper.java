package org.janelia.jacsstorage.service.distributedservice;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

class AgentConnectionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AgentConnectionHelper.class);

    static StorageAgentInfo getAgentStatus(String agentUrl) {
        String agentStatusEndpoint = "/connection/status";
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

    static boolean deleteStorage(String agentUrl, Number dataBundleId, String subject, String authToken) {
        String deleteStorageEndpoint = String.format("/agent-storage/%d", dataBundleId);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(agentUrl)
                        .path(deleteStorageEndpoint);
            Invocation.Builder targetRequestBuilder = target.request()
                    .header("Authorization", "Bearer " + authToken)
                    .header("JacsSubject", subject)
                    ;
            Response response = targetRequestBuilder.delete();
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
