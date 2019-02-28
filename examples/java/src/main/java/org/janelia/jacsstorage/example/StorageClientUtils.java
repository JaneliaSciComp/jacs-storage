package org.janelia.jacsstorage.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.jackson.JacksonFeature;

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
import java.io.InputStream;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class StorageClientUtils {

    static String authenticate(String authUrl, String userName, String password) {
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(authUrl).path("/authenticate");
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

    public static String addNewBundleFolder(String storageURL, Number bundleId, String newDirEntry, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s/directory/%s", bundleId, newDirEntry);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(storageURL).path(dataStreamEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .post(Entity.json("")) // empty request body
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.CREATED.getStatusCode()) {
                return response.getHeaderString("Location");
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public static String addNewBundleFile(String storageURL, Number bundleId, String newFileEntry, InputStream fileContentStream, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s/file/%s", bundleId, newFileEntry);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(storageURL).path(dataStreamEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .post(Entity.entity(fileContentStream, MediaType.APPLICATION_OCTET_STREAM_TYPE))
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.CREATED.getStatusCode()) {
                return response.getHeaderString("Location");
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public static Map<String, Object> allocateBundle(String storageServiceURL, String bundleName, String storageFormat,
                                                     List<String> storageTags, Map<String, String> bundleProperties,
                                                     String authToken) {
        String storageEndpoint = "/storage";
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(storageServiceURL).path(storageEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .post(Entity.json(ImmutableMap.of(
                            "name", bundleName,
                            "storageFormat", storageFormat,
                            "storageTags", storageTags,
                            "metadata", bundleProperties
                    )))
                    ;
            TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>(){};
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.CREATED.getStatusCode()) {
                return response.readEntity(new GenericType<>(typeRef.getType()));
            } else {
                return response.readEntity(new GenericType<>(typeRef.getType()));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public static Map<String, Object> getBundleInfo(String storageServiceURL, Number bundleId, String bundleOwnerKey, String bundleName, String authToken) {
        String storageEndpoint;
        if (bundleId != null && !"0".equals(bundleId.toString())) {
            storageEndpoint = String.format("/storage/%s", bundleId);
        } else {
            storageEndpoint = String.format("/storage/%s/%s", bundleOwnerKey, bundleName);
        }
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(storageServiceURL).path(storageEndpoint);
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
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
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public static List<Map<String, Object>> listBundleContent(String storageURL, Number bundleId, String entryName, String authToken) {
        String storageContentEndpoint = String.format("/agent_storage/%s", bundleId);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(storageURL)
                    .path(storageContentEndpoint)
                    .path("list");
            if (StringUtils.isNotBlank(entryName)) {
                target = target.queryParam("entry", entryName);
            }
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
            int responseStatus = response.getStatus();
            TypeReference<List<Map<String, Object>>> typeRef = new TypeReference<List<Map<String, Object>>>(){};
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return response.readEntity(new GenericType<>(typeRef.getType()));
            } else {
                return response.readEntity(new GenericType<>(typeRef.getType()));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public static Map<String, Object> searchBundles(String storageServiceURL,
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
            httpClient = createHttpClient();
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
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
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
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public static InputStream streamDataEntryFromBundle(String storageURL, Number bundleId, String entryPath, String authToken) {
        String dataStreamEndpoint = String.format("/agent_storage/%s", bundleId);
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(storageURL)
                    .path(dataStreamEndpoint)
                    .path("data_content")
                    .path(entryPath);
            Response response = target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .header("Authorization", "Bearer " + authToken)
                    .get()
                    ;
            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return response.readEntity(InputStream.class);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
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
                .sslContext(sslContext)
                .hostnameVerifier((s, sslSession) -> true)
                .register(new JacksonFeature())
                .build();
    }

}
