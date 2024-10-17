package org.janelia.jacsstorage.clients.api;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.clients.api.n5.N5Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: This code was copied from org.janelia.jacs2.dataservice.storage.StorageService in jacs-compute.
 *       It needs to be cleaned up and refactored in the future.
 */
public class JadeHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(JadeHttpClient.class);

    private final String masterStorageServiceURL;
    private final String storageServiceApiKey;

    public JadeHttpClient(String masterStorageServiceURL, String storageServiceApiKey) {
        this.masterStorageServiceURL = masterStorageServiceURL;
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public Optional<StorageLocation> lookupStorage(String storagePath, String ownerKey, String authToken, JadeStorageAttributes storageAttributes) {
        LOG.debug("Lookup storage for {}", storagePath);
        return findStorageVolumes(storagePath, ownerKey, authToken, storageAttributes)
                .stream().findFirst()
                .map(jadeStorageVolume -> {
                    String relativeStoragePath;
                    if (StringUtils.startsWith(storagePath, StringUtils.appendIfMissing(jadeStorageVolume.getStorageVirtualPath(), "/"))) {
                        relativeStoragePath = Paths.get(jadeStorageVolume.getStorageVirtualPath()).relativize(Paths.get(storagePath)).toString();
                    } else if (StringUtils.startsWith(storagePath, StringUtils.appendIfMissing(jadeStorageVolume.getBaseStorageRootDir(), "/"))) {
                        relativeStoragePath = Paths.get(jadeStorageVolume.getBaseStorageRootDir()).relativize(Paths.get(storagePath)).toString();
                    } else if ("S3".equals(jadeStorageVolume.getStorageType())) {
                        // S3 volumes may have null root dir - in that case renderedVolume
                        // is located at the same path as the one used for searching the volume, i.e. dataPath
                        relativeStoragePath = storagePath;
                    } else {
                        relativeStoragePath = "";
                    }
                    LOG.debug("Found {} for {}; the new path relative to the volume's root is {}", jadeStorageVolume, storagePath, relativeStoragePath);
                    return new StorageLocation(
                            jadeStorageVolume.getVolumeStorageURI(),
                            jadeStorageVolume.getStorageType(),
                            jadeStorageVolume.getBaseStorageRootDir(),
                            jadeStorageVolume.getStorageVirtualPath(),
                            storageAttributes
                    );
                });
    }

    public List<JadeStorageVolume> findStorageVolumes(String storagePath, String subjectKey, String authToken, JadeStorageAttributes storageAttributes) {
        return lookupStorageVolumes(null, null, storagePath, subjectKey, authToken, storageAttributes).stream()
                .filter(vsInfo -> StringUtils.equals(storagePath, vsInfo.getStorageVirtualPath())
                        || StringUtils.equals(storagePath, vsInfo.getBaseStorageRootDir())
                        || StringUtils.startsWith(storagePath, StringUtils.appendIfMissing(vsInfo.getStorageVirtualPath(), "/"))
                        || StringUtils.startsWith(storagePath, StringUtils.appendIfMissing(vsInfo.getBaseStorageRootDir(), "/"))
                        || StringUtils.startsWith("S3", vsInfo.getStorageType()))
                .collect(Collectors.toList());
    }

    public List<JadeStorageVolume> lookupStorageVolumes(String storageId,
                                                        String storageName,
                                                        String storagePath,
                                                        String subjectKey,
                                                        String authToken,
                                                        JadeStorageAttributes storageAttributes) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(masterStorageServiceURL)
                    .path("storage_volumes");
            if (StringUtils.isNotBlank(storageId)) {
                target = target.queryParam("id", storageId);
            }
            if (StringUtils.isNotBlank(storageName)) {
                target = target.queryParam("name", storageName);
            }
            if (StringUtils.isNotBlank(storagePath)) {
                target = target.queryParam("dataStoragePath", storagePath);
            }
            if (StringUtils.isNotBlank(subjectKey)) {
                target = target.queryParam("ownerKey", subjectKey);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(target, MediaType.APPLICATION_JSON, subjectKey, authToken, storageServiceApiKey, storageAttributes);
            Response response = requestBuilder.get();
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.error("Lookup storage volume request {} returned status {} while trying to get the storage for storageId = {}, storageName={}, storagePath={}", target, responseStatus, storageId, storageName, storagePath);
                return Collections.emptyList();
            } else {
                JadeResults<JadeStorageVolume> storageInfoResult = response.readEntity(new GenericType<JadeResults<JadeStorageVolume>>() {
                });
                return storageInfoResult.getResultList();
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public List<StorageObject> listStorageContent(StorageLocation storageLocation, String relativePath, int depth, String subjectKey, String authToken) throws StorageObjectNotFoundException {
        Client httpclient = HttpUtils.createHttpClient();
        String storageURL = storageLocation.getStorageURL();
        try {
            WebTarget target = httpclient.target(storageURL).path("list");
            if (StringUtils.isNotBlank(relativePath)) {
                target = target.path(relativePath);
            } else {
                target = target.path("/");
            }
            if (depth > 0) {
                target = target.queryParam("depth", depth);
            }
            LOG.debug("listStorageContent requesting {}", target.getUri().toString());
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target, MediaType.APPLICATION_JSON, subjectKey, authToken, storageServiceApiKey, storageLocation.getStorageAttributes()
            );
            Response response = requestBuilder.get();
            if (response.getStatus() == 404) {
                throw new StorageObjectNotFoundException(storageLocation, relativePath);
            }
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
            return response.readEntity(new GenericType<List<JsonNode>>() {
                    })
                    .stream()
                    .map(content -> {
                        StorageEntryInfo storageEntryInfo = extractStorageNodeFromJson(storageURL, null, relativePath, content);
                        String objectRelativePath = StringUtils.isBlank(relativePath) ? relativePath : StringUtils.appendIfMissing(relativePath, "/");
                        return new StorageObject(storageLocation, objectRelativePath + storageEntryInfo.getEntryRelativePath(), storageEntryInfo);
                    })
                    .collect(Collectors.toList());
        } finally {
            httpclient.close();
        }
    }

    public InputStream getStorageContent(String storageURI, String subject, String authToken, JadeStorageAttributes storageAttributes) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target, null, subject, authToken, storageServiceApiKey, storageAttributes);
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(storageURI + " returned with " + response.getStatus());
            }
            return response.readEntity(InputStream.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    protected void setStorageContent(String storageURI, String subjectKey, String authToken, JadeStorageAttributes storageAttributes, InputStream fileStream) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target, null, subjectKey, authToken, storageServiceApiKey, storageAttributes);
            LOG.debug("setStorageContent putting to {}", target.getUri().toString());
            Response response = requestBuilder.put(Entity.entity(fileStream, MediaType.APPLICATION_OCTET_STREAM));
            if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public boolean exists(String storageURI, String subjectKey, String authToken, JadeStorageAttributes storageAttributes) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI);
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target, MediaType.APPLICATION_JSON, subjectKey, authToken, storageServiceApiKey, storageAttributes
            );
            Response response = requestBuilder.head();
            if (response.getStatus() >= 200 && response.getStatus() < 300) {
                return true;
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                return false;
            } else {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

//    public long getContentLength(String storageURI, String subjectKey, String authToken, JadeStorageAttributes storageAttributes) {
//        Client httpclient = HttpUtils.createHttpClient();
//        try {
//            WebTarget target = httpclient.target(storageURI);
//            Invocation.Builder requestBuilder = createRequestWithCredentials(
//                    target.request(MediaType.APPLICATION_JSON), subjectKey, authToken, storageAttributes
//            );
//            Response response = requestBuilder.head();
//            if (response.getStatus() >= 200 && response.getStatus() < 300) {
//                return response.getLength();
//            } else {
//                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
//            }
//        } catch (IllegalStateException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new IllegalStateException(e);
//        } finally {
//            httpclient.close();
//        }
//    }

    StorageEntryInfo extractStorageNodeFromJson(String storageUrl, String storageEntryUrl, String storagePath, JsonNode jsonNode) {
        JsonNode storageIdNode = jsonNode.get("storageId");
        JsonNode storageRootLocationNode = jsonNode.get("storageRootLocation");
        JsonNode storageRootPathURINode = jsonNode.get("storageRootPathURI");
        JsonNode nodeAccessURLNode = jsonNode.get("nodeAccessURL");
        JsonNode nodeRelativePathNode = jsonNode.get("nodeRelativePath");
        JsonNode collectionFlagNode = jsonNode.get("collectionFlag");
        JsonNode mimeTypeNode = jsonNode.get("mimeType");
        JsonNode sizeNode = jsonNode.get("size");
        String storageId = null;
        if (storageIdNode != null && !storageIdNode.isNull()) {
            storageId = storageIdNode.asText();
        }
        String actualEntryURL;
        if (nodeAccessURLNode != null && StringUtils.isNotBlank(nodeAccessURLNode.asText())) {
            actualEntryURL = nodeAccessURLNode.asText();
        } else if (StringUtils.isNotBlank(storageEntryUrl)) {
            actualEntryURL = storageEntryUrl;
        } else {
            if (StringUtils.isNotBlank(storagePath)) {
                actualEntryURL = StringUtils.appendIfMissing(storageUrl, "/") + storagePath;
            } else {
                actualEntryURL = storageUrl;
            }
        }
        return new StorageEntryInfo(
                storageId,
                storageUrl,
                actualEntryURL,
                storageRootLocationNode.asText(),
                new StoragePathURI(storageRootPathURINode.asText()),
                nodeRelativePathNode.asText(),
                sizeNode.asLong(),
                collectionFlagNode.asBoolean(),
                mimeTypeNode.asText());
    }

    /**
     * TODO: this doesn't work yet because it turns out that N5TreeNode is not serializable
     * Discover n5 data sets and return a tree of N5 objects metadata for the given object.
     *
     * @param storageLocation location of the object
     * @param relativePath    path to the object relative to the storageLocation
     * @return tree of data sets represented by N5TreeNode
     */
    public N5Node getN5Tree(StorageLocation storageLocation, String relativePath, String jacsPrincipal, String authToken, JadeStorageAttributes storageAttributes) throws StorageObjectNotFoundException {
        Client httpclient = HttpUtils.createHttpClient();
        String storageURL = storageLocation.getStorageURL();
        try {
            WebTarget target = httpclient.target(storageURL).path("n5tree");
            if (StringUtils.isNotBlank(relativePath)) {
                target = target.path(relativePath);
            } else {
                target = target.path("/");
            }
            LOG.debug("getN5Tree requesting {}", target.getUri().toString());
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target, MediaType.APPLICATION_JSON, jacsPrincipal, authToken, storageServiceApiKey, storageAttributes
            );
            Response response = requestBuilder.get();
            if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new StorageObjectNotFoundException(storageLocation, relativePath);
            }
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
            return response.readEntity(new GenericType<N5Node>() {
            });
        } finally {
            httpclient.close();
        }
    }

    private Invocation.Builder createRequestWithCredentials(WebTarget target,
                                                            String mediaType,
                                                            String jacsPrincipal,
                                                            String bearerToken,
                                                            String serviceApiKey,
                                                            JadeStorageAttributes storageAttributes) {
        Invocation.Builder requestWithCredentialsBuilder = StringUtils.isNotBlank(mediaType)
                ? target.request(mediaType)
                : target.request();
        if (StringUtils.isNotBlank(bearerToken)) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "Authorization",
                    "Bearer " + bearerToken);
        } else if (StringUtils.isNotBlank(serviceApiKey)) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "Authorization",
                    "APIKEY " + serviceApiKey);
        }
        if (StringUtils.isNotBlank(jacsPrincipal)) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "JacsSubject",
                    jacsPrincipal);
        }
        if (storageAttributes != null) {
            for (String storageAttribute : storageAttributes.getAttributeNames()) {
                requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                        storageAttribute,
                        storageAttributes.getAttributeValue(storageAttribute));
            }
        }
        return requestWithCredentialsBuilder;
    }

}
