package org.janelia.jacsstorage.clientutils;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
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
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.InvocationCallback;
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
import java.util.concurrent.Future;

public class StorageClientImplHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StorageClientImplHelper.class);

    private final PoolingHttpClientConnectionManager connectionManager;

    public StorageClientImplHelper() {
        this.connectionManager = createConnectionManager();
    }

    public Optional<DataStorageInfo> allocateStorage(String connectionURL, DataStorageInfo storageRequest, String authToken) {
        String storageEndpoint = "/storage";
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(storageEndpoint);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON_TYPE),
                    authToken,
                    null)
                    .post(Entity.json(storageRequest))
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.CREATED.getStatusCode()) {
                return Optional.of(response.readEntity(DataStorageInfo.class));
            } else {
                Map<String, String> errResponse = response.readEntity(new GenericType<>(new TypeReference<Map<String, String>>(){}.getType()));
                LOG.warn("Allocate storage request {} returned with status {} - {}", target.getUri(), responseStatus, errResponse);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpClient.close();
        }
    }

    public long countStorageRecords(String connectionURL, Number bundleId, String authToken) {
        String storageEndpoint = "/storage/size";
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(storageEndpoint);
            if (bundleId != null) {
                target = target.queryParam("id", bundleId);
            }
            LOG.debug("Count {} as {}", target, authToken);
            Response response = createRequestWithCredentials(target.request(),
                    authToken,
                    null)
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
            httpClient.close();
        }
    }

    public PageResult<DataStorageInfo> listStorageRecords(String connectionURL, String storageHost, List<String> storageTags, Number bundleId, PageRequest request, String authToken) {
        String storageEndpoint = "/storage";
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
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
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON_TYPE),
                    authToken,
                    null)
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
            httpClient.close();
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
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(storageEndpoint);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON_TYPE),
                    authToken,
                    null)
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
            httpClient.close();
        }
    }

    public List<DataNodeInfo> listStorageContent(String connectionURL, Number bundleId, String authToken) {
        String storageContentEndpoint = String.format("/agent_storage/%s", bundleId);
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL)
                    .path(storageContentEndpoint)
                    .path("list");
            LOG.debug("List content {} as {}", target, authToken);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON_TYPE),
                    authToken,
                    null)
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
            httpClient.close();
        }
    }

    public Optional<DataStorageInfo> streamDataToStore(String connectionURL, DataStorageInfo storageInfo, InputStream dataStream, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s", storageInfo.getId());
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            LOG.debug("Stream data to {} as {}", target, authToken);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON_TYPE),
                    authToken,
                    null)
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
            httpClient.close();
        }
    }

    public Optional<InputStream> streamDataFromStore(String connectionURL, Number storageId, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s", storageId);
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            LOG.debug("Stream data from {} as {}", target, authToken);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE),
                    authToken,
                    null)
                    .get();
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return Optional.of(response.readEntity(InputStream.class));
            } else {
                LOG.warn("Stream data returned with status {} for {}", responseStatus, target);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpClient.close();
        }
    }

    public Optional<InputStream> streamDataEntryFromStorage(String connectionURL, Number storageId, String entryPath, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s", storageId);
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL)
                    .path(dataStreamEndpoint)
                    .path("entry_content")
                    .path(entryPath);
            LOG.debug("Stream data entry from {} as {}", target, authToken);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE),
                    authToken,
                    null)
                    .get();
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return Optional.of(response.readEntity(InputStream.class));
            } else {
                LOG.warn("Stream data returned with status {} for {}", responseStatus, target);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpClient.close();
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

    private Optional<String> addNewStorageFolder(String connectionURL, Number storageId, String newDirEntryPath, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s/directory/%s", storageId, newDirEntryPath);
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON_TYPE),
                    authToken,
                    null)
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
            httpClient.close();
        }
    }

    public Optional<String> createNewFile(String connectionURL, Number dataBundleId, String newFilePath, InputStream contentStream, String authToken) {
        LOG.debug("Create new file {}:{}:{} ({})", connectionURL, dataBundleId, newFilePath, authToken);
        DataStorageInfo storageRequest = new DataStorageInfo().setNumericId(dataBundleId);
        return retrieveStorageInfo(connectionURL, storageRequest, authToken)
                .flatMap((DataStorageInfo storageInfo) -> {
                    String agentStorageServiceURL = storageInfo.getConnectionURL();
                    LOG.debug("Invoke agent {} to create new file {}/{}", agentStorageServiceURL, dataBundleId, newFilePath);
                    return addNewStorageContent(agentStorageServiceURL, storageInfo.getNumericId(), newFilePath, contentStream, authToken);
                });
    }

    private Optional<String> addNewStorageContent(String connectionURL, Number storageId, String newFileEntryPath, InputStream contentStream, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s/file/%s", storageId, newFileEntryPath);
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL).path(dataStreamEndpoint);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON_TYPE),
                    authToken,
                    null)
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
            httpClient.close();
        }
    }

    public StorageMessageResponse ping(String connectionURL) throws IOException {
        String endpoint = "/storage/status";
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
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
            httpClient.close();
        }
    }

    public Map<String, Object> searchBundles(String storageServiceURL,
                                             Number bundleId,
                                             String bundleOwnerKey,
                                             String bundleName,
                                             String storageHost,
                                             List<String> storageTags,
                                             int pageNumber,
                                             int pageSize,
                                             String authToken) {
        String storageEndpoint = "/storage";
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(storageServiceURL).path(storageEndpoint);
            if (bundleId != null && !"0".equals(bundleId.toString())) {
                target = target.queryParam("id", bundleId);
            }
            if (StringUtils.isNotBlank(bundleOwnerKey)) {
                target = target.queryParam("ownerKey", bundleOwnerKey);
            }
            if (StringUtils.isNotBlank(bundleName)) {
                target = target.queryParam("name", bundleName);
            }
            if (StringUtils.isNotBlank(storageHost)) {
                target = target.queryParam("storageHost", storageHost);
            }
            if (!storageTags.isEmpty()) {
                target = target.queryParam("storageTags", storageHost);
            }
            if (pageNumber > 0) {
                target = target.queryParam("page", pageNumber);
            }
            if (pageSize > 0) {
                target = target.queryParam("length", pageSize);
            }
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON_TYPE),
                    authToken,
                    null)
                    .get();
            int responseStatus = response.getStatus();
            TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>(){};
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return response.readEntity(new GenericType<>(typeRef.getType()));
            } else {
                return response.readEntity(new GenericType<>(typeRef.getType()));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpClient.close();
        }
    }

    public Optional<InputStream> streamPathContentFromMaster(String connectionURL, String dataPath, String authToken) {
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL)
                    .path("/storage_content/storage_path_redirect")
                    .path(dataPath)
                    .property(ClientProperties.FOLLOW_REDIRECTS, true);
            LOG.debug("Stream data entry from {} as {}", target, authToken);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE),
                    authToken,
                    null)
                    .get();
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                InputStream responseStream = response.readEntity(InputStream.class);
                return Optional.of(responseStream);
            } else {
                LOG.warn("Stream data returned with status {} for {}", responseStatus, target);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpClient.close();
        }
    }

    public boolean checkPathContentFromMaster(String connectionURL, String dataPath, String authToken) {
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL)
                    .path("/storage_content/storage_path_redirect")
                    .path(dataPath)
                    .property(ClientProperties.FOLLOW_REDIRECTS, true);
            LOG.debug("Stream data entry from {} as {}", target, authToken);
            Response response = createRequestWithCredentials(target.request(MediaType.TEXT_PLAIN),
                    authToken,
                    null)
                    .head();
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return true;
            } else {
                LOG.warn("Stream data returned with status {} for {}", responseStatus, target);
                return false;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpClient.close();
        }
    }

    public Future<InputStream> asyncStreamPathContentFromMaster(String connectionURL, String dataPath, String authToken, InvocationCallback<InputStream> callback) {
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL)
                    .path("/storage_content/storage_path_redirect")
                    .path(dataPath)
                    .property(ClientProperties.FOLLOW_REDIRECTS, true);
            LOG.debug("Stream data entry from {} as {}", target, authToken);
            return createRequestWithCredentials(target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE),
                    authToken,
                    null)
                    .async()
                    .get(callback);
        } finally {
            httpClient.close();
        }
    }

    public Optional<InputStream> streamPathContentFromAgent(String connectionURL, String dataPath, String authToken) {
        Client httpClient = null;
        try {
            httpClient = createNewHttpClient();
            WebTarget target = httpClient.target(connectionURL)
                    .path("/agent_storage/storage_path")
                    .path(dataPath);
            LOG.debug("Stream data entry from {} as {}", target, authToken);
            Response response = createRequestWithCredentials(target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE),
                    authToken,
                    null)
                    .get();
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return Optional.of(response.readEntity(InputStream.class));
            } else {
                LOG.warn("Stream data returned with status {} for {}", responseStatus, target);
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpClient.close();
        }
    }

    private Client createNewHttpClient() {
        // values are in milliseconds
        final RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(120000)
                .setConnectTimeout(300000)
                .setSocketTimeout(300000)
                .build();

        ClientConfig clientConfig = new ClientConfig()
                .connectorProvider(new ApacheConnectorProvider())
                .property(ApacheClientProperties.CONNECTION_MANAGER, connectionManager)
                .property(ApacheClientProperties.CONNECTION_MANAGER_SHARED, true)
                .property(ApacheClientProperties.REQUEST_CONFIG, requestConfig)
                ;

        return ClientBuilder.newBuilder()
                .withConfig(clientConfig)
                .register(new JacksonFeature())
                .build();
    }

    private PoolingHttpClientConnectionManager createConnectionManager() {
        try {
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
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", new SSLConnectionSocketFactory(sslContext, (s, sslSession) -> true))
                    .build();
            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            connectionManager.setMaxTotal(1000);

            connectionManager.setDefaultSocketConfig(SocketConfig.custom()
                    .setSoReuseAddress(true)
                    .setSoKeepAlive(true)
                    .setRcvBufSize(16384)
                    .build());
            return connectionManager;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder, String authToken, String jacsPrincipal) {
        Invocation.Builder requestWithCredentialsBuilder = requestBuilder;
        if (StringUtils.isNotBlank(authToken)) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "Authorization",
                    "Bearer " + authToken);
        }
        if (StringUtils.isNotBlank(jacsPrincipal)) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "JacsSubject",
                    jacsPrincipal);
        }
        return requestWithCredentialsBuilder;
    }

}
