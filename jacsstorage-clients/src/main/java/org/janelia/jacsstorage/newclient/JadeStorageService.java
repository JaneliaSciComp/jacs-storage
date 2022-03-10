package org.janelia.jacsstorage.newclient;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A client for JADE which exposes the service as a file-like API.
 *
 * Builds on top of StorageContentHelper to create a better storage API by abstracting away JADE implementation details
 * and letting the user deal with JADE as a file object storage service.
 */
public class JadeStorageService extends StorageContentHelper {

    private static final Logger LOG = LoggerFactory.getLogger(StorageContentHelper.class);

    private String subjectKey;
    private String authToken;

    /**
     *
     * @param storageService
     * @param subjectKey subject key of the user
     * @param authToken authentication token for JADE
     */
    public JadeStorageService(StorageService storageService, String subjectKey, String authToken) {
        super(storageService);
        this.subjectKey = subjectKey;
        this.authToken = authToken;
    }

    /**
     * Find the StorageLocation of the given path. If no storage location exists, null is returned.
     * @param path full JADE path to locate
     * @return
     */
    public StorageLocation getStorageObjectByPath(String path) {
        return lookupStorage(path, subjectKey, authToken).map(StorageLocation::new).orElse(null);
    }

    /**
     * Retrieve metadata for the given object.
     * @param storageLocation location of the object
     * @param relativePath path to the object relative to the storageLocation
     * @return
     */
    public StorageObject getMetadata(StorageLocation storageLocation, String relativePath) throws StorageObjectNotFoundException {
        return listStorageContent(storageLocation, relativePath, 0)
                .stream()
                .peek(storageObject -> LOG.trace("getMetadata found {}",storageObject))
                .findFirst()
                .orElseThrow(() -> new StorageObjectNotFoundException(storageLocation, relativePath));
    }

    /**
     * List the immediate children of the given storage object. If the given object doesn't exist, then an
     * IllegalArgumentException will be thrown. If the given object is not a collection, then an empty list will be
     * returned.
     * @param storageLocation location of the object
     * @param relativePath path to the object relative to the storageLocation
     * @return list of child objects
     */
    public List<StorageObject> getChildren(StorageLocation storageLocation, String relativePath) throws StorageObjectNotFoundException {
        return getDescendants(storageLocation, relativePath, 1)
                .stream()
                // We're only interested in children, so filter out the blank relative path which represents the
                // root object, or any path which is the same as the path we asked for.
                .filter(c -> StringUtils.isNotBlank(c.getObjectName()) && !c.getRelativePath().equals(StringUtils.appendIfMissing(relativePath, "/")))
                .collect(Collectors.toList());
    }

    /**
     * List the child objects of the given storage object. If the given object doesn't exist, then an
     * IllegalArgumentException will be thrown. If the given object is not a collection, then an empty list will be
     * returned.
     * @param storageLocation location of the object
     * @param relativePath path to the object relative to the storageLocation
     * @return list of descendant objects
     */
    public List<StorageObject> getDescendants(StorageLocation storageLocation, String relativePath, int depth) throws StorageObjectNotFoundException {
        return listStorageContent(storageLocation, relativePath, depth)
                .stream()
                // We're only interested in descendants, so filter out the blank relative path which represents the
                // root object, or any path which is the same as the path we asked for.
                .filter(c -> StringUtils.isNotBlank(c.getObjectName()) && !c.getRelativePath().equals(StringUtils.appendIfMissing(relativePath, "/")))
                .collect(Collectors.toList());
    }

    public List<StorageObject> listStorageContent(StorageLocation storageLocation, String relativePath, int depth) throws StorageObjectNotFoundException {
        Client httpclient = HttpUtils.createHttpClient();
        String storageURL = storageLocation.getStorageURL();
        try {
            WebTarget target = httpclient.target(storageURL).path("list");
            if (StringUtils.isNotBlank(relativePath)) {
                target = target.path(relativePath);
            }
            if (depth > 0) {
                target = target.queryParam("depth", depth);
            }
            LOG.debug("listStorageContent requesting {}", target.getUri().toString());
            Invocation.Builder requestBuilder = storageService.createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subjectKey, authToken);
            Response response = requestBuilder.get();
            if (response.getStatus()==404) {
                throw new StorageObjectNotFoundException(storageLocation, relativePath);
            }
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
            return response.readEntity(new GenericType<List<JsonNode>>(){})
                    .stream()
                    .map(content -> {
                        StorageEntryInfo storageEntryInfo = storageService.extractStorageNodeFromJson(storageURL, null, relativePath, content);
                        return new StorageObject(storageLocation, relativePath+"/"+storageEntryInfo.getEntryRelativePath(), storageEntryInfo);
                    })
                    .collect(Collectors.toList());
        }
        finally {
            httpclient.close();
        }
    }

    /**
     * Return whether or not the given object exists in storage.
     * @return true if the path exists
     */
    public boolean exists(StorageLocation storageLocation, String relativePath) {
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("exists requesting "+contentURL);
        return storageService.exists(contentURL, subjectKey, authToken);
    }

    /**
     * Returns the content of the given object. The object should be a file, not a collection.
     * @param storageLocation location of the object
     * @param relativePath path to the object relative to the storageLocation
     * @return stream of the content in the given object
     */
    public InputStream getContent(StorageLocation storageLocation, String relativePath) {
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("getContent requesting "+contentURL);
        return storageService.getStorageContent(contentURL, subjectKey, authToken);
    }

    /**
     * Returns the content of the given object as a UTF-8 string. The object should be a file, not a collection.
     * @param storageLocation location of the object
     * @param relativePath path to the object relative to the storageLocation
     * @return stream of the content in the given object
     */
    public String getContentAsString(StorageLocation storageLocation, String relativePath) throws IOException {
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("getContentAsString requesting "+contentURL);
        InputStream stream = storageService.getStorageContent(contentURL, subjectKey, authToken);
        return IOUtils.toString(stream, StandardCharsets.UTF_8);
    }

    /**
     * Returns the size in bytes of the given object.
     * @param storageLocation location of the object
     * @param relativePath path to the object relative to the storageLocation
     * @return stream of the content in the given object
     */
    public long getContentLength(StorageLocation storageLocation, String relativePath) {
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("getContentLength requesting "+contentURL);
        return  storageService.getContentLength(contentURL, subjectKey, authToken);
    }
}
