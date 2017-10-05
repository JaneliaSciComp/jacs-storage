package org.janelia.jacsstorage.agent;

import org.glassfish.jersey.jackson.JacksonFeature;
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
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

class AgentConnectionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AgentConnectionHelper.class);

    static StorageAgentInfo findRegisteredAgent(String masterServiceUrl, String agentLocation) {
        String registrationEndpoint = String.format("/agents/%s", agentLocation);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(masterServiceUrl).path(registrationEndpoint);
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

    static boolean registerAgent(String masterServiceUrl, StorageAgentInfo agentInfo) {
        String registrationEndpoint = "/agents";
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(masterServiceUrl).path(registrationEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .post(Entity.json(agentInfo))
                    ;

            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return true;
            }
            LOG.warn("Register agent returned {}", responseStatus);
        } catch (Exception e) {
            LOG.error("Error while registering {}", agentInfo, e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return false;
    }

    static void deregisterAgent(String masterServiceUrl, String agentLocation) {
        String registrationEndpoint = String.format("/agents/%s", agentLocation);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(masterServiceUrl).path(registrationEndpoint);
            Response response = target.request()
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
