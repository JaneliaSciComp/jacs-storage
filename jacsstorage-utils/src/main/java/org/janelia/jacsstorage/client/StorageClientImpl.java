package org.janelia.jacsstorage.client;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.protocol.StorageMessageResponse;
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
import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Optional;

public class StorageClientImpl implements StorageClient {
    private static final Logger LOG = LoggerFactory.getLogger(StorageClientImpl.class);
    private StorageClient storageClient;

    public StorageClientImpl(StorageClient storageClient) {
        this.storageClient = storageClient;
    }

    public StorageMessageResponse persistData(String localPath, DataStorageInfo storageInfo) throws IOException {
        DataStorageInfo allocatedStorage = allocateStorage(storageInfo);
        StorageMessageResponse storageResponse = storageClient.persistData(localPath, allocatedStorage);
        if (storageResponse.getStatus() == StorageMessageResponse.OK) {
            updateStorageInfo(storageInfo.getConnectionInfo(), storageResponse.getPersistedBytes(), allocatedStorage);
        }
        return storageResponse;
    }

    private DataStorageInfo allocateStorage(DataStorageInfo storageRequest) {
        String storageEndpoint = "/storage";
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(storageRequest.getConnectionInfo()).path(storageEndpoint);

            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .post(Entity.json(storageRequest))
                    ;

            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.CREATED.getStatusCode()) {
                return response.readEntity(DataStorageInfo.class);
            } else {
                LOG.warn("Allocate storage request returned with status {}", responseStatus);
                throw new IllegalStateException("Error while trying to allocate data storage - returned status: " + responseStatus);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    private Optional<DataStorageInfo> updateStorageInfo(String connectionInfo, long messageSize, DataStorageInfo storageInfo) {
        String storageEndpoint = String.format("/storage/%d", storageInfo.getId());
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionInfo).path(storageEndpoint);
            // set the fields to update
            DataStorageInfo storageUpdate = new DataStorageInfo();
            storageUpdate.setId(storageInfo.getId());
            storageUpdate.setRequestedSpaceInKB(messageSize);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .put(Entity.json(storageUpdate))
                    ;

            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return Optional.of(response.readEntity(DataStorageInfo.class));
            } else {
                LOG.warn("Update storage info returned with status {}", responseStatus);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return Optional.empty();
    }

    @Override
    public StorageMessageResponse retrieveData(String localPath, DataStorageInfo storageInfo) throws IOException {
        DataStorageInfo allocatedStorage = retrieveStorageInfo(storageInfo);
        return storageClient.retrieveData(localPath, allocatedStorage);
    }

    private DataStorageInfo retrieveStorageInfo(DataStorageInfo storageRequest) {
        String storageEndpoint = String.format("/storage/%s/%s", storageRequest.getOwner(), storageRequest.getName());
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(storageRequest.getConnectionInfo()).path(storageEndpoint);

            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .get()
                    ;

            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return response.readEntity(DataStorageInfo.class);
            } else {
                LOG.warn("Retrieve storage info request returned with status {}", responseStatus);
                throw new IllegalStateException("Error while trying to retrieve data storage - returned status: " + responseStatus);
            }
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