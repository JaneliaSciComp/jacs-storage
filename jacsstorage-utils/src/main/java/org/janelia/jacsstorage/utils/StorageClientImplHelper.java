package org.janelia.jacsstorage.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.service.StorageMessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Optional;

public class StorageClientImplHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StorageClientImplHelper.class);
    private static final String AUTH_URL = "http://api.int.janelia.org:8030";

    public Optional<DataStorageInfo> allocateStorage(String connectionURL, DataStorageInfo storageRequest, String authToken) {
        String storageEndpoint = "/storage";
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(storageEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .post(Entity.json(storageRequest))
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.CREATED.getStatusCode()) {
                return Optional.of(response.readEntity(DataStorageInfo.class));
            } else {
                Map<String, String> errResponse = response.readEntity(Map.class);
                LOG.warn("Allocate storage request {} returned with status {} - {}", target.getUri(), responseStatus, errResponse);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public Optional<DataStorageInfo> retrieveStorageInfo(String connectionURL, DataStorageInfo storageRequest, String authToken) {
        String storageEndpoint = String.format("/storage/%s/%s", storageRequest.getOwner(), storageRequest.getName());
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(storageEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return Optional.of(response.readEntity(DataStorageInfo.class));
            } else {
                LOG.warn("Retrieve storage info request returned with status {}", responseStatus);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public Optional<DataStorageInfo> streamDataToStore(String connectionURL, DataStorageInfo storageInfo, InputStream dataStream, String authToken) {
        String dataStreamEndpoint = String.format("/agent-storage/%s", storageInfo.getId());
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .post(Entity.entity(dataStream, MediaType.APPLICATION_OCTET_STREAM_TYPE))
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return Optional.of(response.readEntity(DataStorageInfo.class));
            } else {
                LOG.warn("Stream data from {} returned with status {}", target, responseStatus);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public Optional<InputStream> streamDataFromStore(String connectionURL, DataStorageInfo storageInfo, String authToken) {
        String dataStreamEndpoint = String.format("/agent-storage/%s", storageInfo.getId());
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            Response response = target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return Optional.of(response.readEntity(InputStream.class));
            } else {
                LOG.warn("Stream data returned with status {}", responseStatus);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public void createNewDirectory(Number dataBundleId, String newDirPath, String authToken) {
        // !!!!!!!!!!!!!!!!!!!!!!TODO
    }

    public StorageMessageResponse ping(String connectionURL) throws IOException {
        String endpoint = "/storage/status";
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(endpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .get()
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return new StorageMessageResponse(StorageMessageResponse.OK, "");
            } else {
                return new StorageMessageResponse(StorageMessageResponse.ERROR, "Response status: " + responseStatus);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public String authenticate(String userName, String password) {
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(AUTH_URL).path("/authenticate");
            TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>(){};
            Map<String, String> tokenResponse = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .post(Entity.json(ImmutableMap.of(
                            "username", userName,
                            "password", password
                    )), new GenericType<>(typeRef.getType()))
                    ;
            return tokenResponse.get("token");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    private Client createHttpClient() throws Exception {
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
                .sslContext(sslContext)
                .hostnameVerifier((s, sslSession) -> true)
                .register(new JacksonFeature())
                .build();
    }

}
