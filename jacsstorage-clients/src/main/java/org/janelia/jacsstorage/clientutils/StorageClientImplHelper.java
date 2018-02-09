package org.janelia.jacsstorage.clientutils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datatransfer.StorageMessageResponse;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StorageClientImplHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StorageClientImplHelper.class);

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
                @SuppressWarnings("unchecked")
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

    public long countStorageRecords(String connectionURL, Number bundleId, String authToken) {
        String storageEndpoint = "/storage/size";
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(storageEndpoint);
            if (bundleId != null) {
                target = target.queryParam("id", bundleId);
            }
            LOG.debug("Count {} as {}", target, authToken);
            Response response = target.request()
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                String value = response.readEntity(String.class);
                return Long.parseLong(value);
            } else {
                LOG.warn("Retrieve storage info request returned with status {}", responseStatus);
                return -1;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public PageResult<DataStorageInfo> listStorageRecords(String connectionURL, String storageHost, List<String> storageTags, Number bundleId, PageRequest request, String authToken) {
        String storageEndpoint = "/storage";
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(storageEndpoint);
            if (bundleId != null && !"0".equals(bundleId.toString())) {
                target = target.queryParam("id", bundleId);
            }
            if (StringUtils.isNotBlank(storageHost)) {
                target = target.queryParam("storageHost", storageHost);
            }
            if (CollectionUtils.isNotEmpty(storageTags)) {
                target = target.queryParam("storageTags", storageHost);
            }
            if (request.getPageNumber() > 0) {
                target = target.queryParam("page", request.getPageNumber());
            }
            if (request.getPageSize() > 0) {
                target = target.queryParam("length", request.getPageSize());
            }
            LOG.debug("List {} as {}", target, authToken);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return response.readEntity(new GenericType<PageResult<DataStorageInfo>>() {});
            } else {
                LOG.warn("Retrieve storage info request returned with status {}", responseStatus);
                return new PageResult<>(request, Collections.emptyList());
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
        String storageEndpoint;
        if (storageRequest.hasId()) {
            storageEndpoint = String.format("/storage/%s", storageRequest.getId());
        } else {
            storageEndpoint = String.format("/storage/%s/%s", storageRequest.getOwnerKey(), storageRequest.getName());
        }
        LOG.debug("Retrieve storage info from {} using {} as {}", connectionURL, storageEndpoint, authToken);

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

    public List<DataNodeInfo> listStorageContent(String connectionURL, Number bundleId, String authToken) {
        String storageContentyEndpoint = String.format("/agent_storage/%s", bundleId);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL)
                    .path(storageContentyEndpoint)
                    .path("list");
            LOG.debug("List content {} as {}", target, authToken);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return response.readEntity(new GenericType<List<DataNodeInfo>>() {});
            } else {
                LOG.warn("Retrieve storage info request returned with status {}", responseStatus);
                return Collections.emptyList();
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
        String dataStreamEndpoint = String.format("/agent_storage/%s", storageInfo.getId());
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            LOG.debug("Stream data to {} as {}", target, authToken);
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

    public Optional<InputStream> streamDataFromStore(String connectionURL, Number storageId, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s", storageId);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            LOG.debug("Stream data from {} as {}", target, authToken);
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

    public Optional<InputStream> streamDataEntryFromStorage(String connectionURL, Number storageId, String entryPath, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s", storageId);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL)
                    .path(dataStreamEndpoint)
                    .path("entry_content")
                    .path(entryPath);
            LOG.debug("Stream data entry from {} as {}", target, authToken);
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

    public Optional<String> createNewDirectory(String connectionURL, Number dataBundleId, String newDirPath, String authToken) {
        LOG.debug("Create new directory {}:{}:{}", connectionURL, dataBundleId, newDirPath);
        DataStorageInfo storageRequest = new DataStorageInfo().setNumericId(dataBundleId);
        return retrieveStorageInfo(connectionURL, storageRequest, authToken)
            .flatMap((DataStorageInfo storageInfo) -> {
                String agentStorageServiceURL = storageInfo.getConnectionURL();
                LOG.debug("Invoke agent {} to create new directory {}/{}", agentStorageServiceURL, dataBundleId, newDirPath);
                return addNewStorageFolder(agentStorageServiceURL, storageInfo.getNumericId(), newDirPath, authToken);
            });
    }

    private Optional<String> addNewStorageFolder(String connectionURL, Number storageId, String newDirPath, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s/directory/%s", storageId, newDirPath);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .post(Entity.json("")) // empty request body
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.CREATED.getStatusCode()) {
                return Optional.of(response.getHeaderString("Location"));
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

    public Optional<String> createNewFile(String connectionURL, Number dataBundleId, String newDirPath, InputStream contentStream, String authToken) {
        LOG.debug("Create new file {}:{}:{} ({})", connectionURL, dataBundleId, newDirPath, authToken);
        DataStorageInfo storageRequest = new DataStorageInfo().setNumericId(dataBundleId);
        return retrieveStorageInfo(connectionURL, storageRequest, authToken)
                .flatMap((DataStorageInfo storageInfo) -> {
                    String agentStorageServiceURL = storageInfo.getConnectionURL();
                    LOG.debug("Invoke agent {} to create new file {}/{}", agentStorageServiceURL, dataBundleId, newDirPath);
                    return addNewStorageContent(agentStorageServiceURL, storageInfo.getNumericId(), newDirPath, contentStream, authToken);
                });
    }

    private Optional<String> addNewStorageContent(String connectionURL, Number storageId, String newDirPath, InputStream contentStream, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s/file/%s", storageId, newDirPath);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .post(Entity.entity(contentStream, MediaType.APPLICATION_OCTET_STREAM_TYPE))
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.CREATED.getStatusCode()) {
                return Optional.of(response.getHeaderString("Location"));
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
