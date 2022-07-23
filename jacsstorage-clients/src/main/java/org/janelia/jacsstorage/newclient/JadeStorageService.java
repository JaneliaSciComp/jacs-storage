package org.janelia.jacsstorage.newclient;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.saalfeldlab.n5.N5TreeNode;
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
 * Builds on top of StorageService to create a better storage API by abstracting away JADE implementation details
 * and letting the user deal with JADE as a file object storage service.
 */
public class JadeStorageService {

    private static final Logger LOG = LoggerFactory.getLogger(JadeStorageService.class);

    private final StorageService storageService;

    private final String subjectKey;
    private final String authToken;

    public JadeStorageService(String masterStorageServiceURL, String storageServiceApiKey) {
        this.storageService = new StorageService(masterStorageServiceURL, storageServiceApiKey);
        this.subjectKey = null;
        this.authToken = null;
    }

    public JadeStorageService(StorageService storageService, String subjectKey, String authToken) {
        this.storageService = storageService;
        this.subjectKey = subjectKey;
        this.authToken = authToken;
    }

    /**
     * @deprecated This method's name is a misnomer. Use getStorageLocationByPath instead.
     */
    @Deprecated
    public StorageLocation getStorageObjectByPath(String path) {
        return getStorageLocationByPath(path);
    }

    /**
     * Find the StorageLocation of the given path. If no storage location exists, null is returned.
     * @param path full JADE path to locate
     * @return
     */
    public StorageLocation getStorageLocationByPath(String path) {
        return storageService.lookupStorage(path, subjectKey, authToken).map(StorageLocation::new).orElse(null);
    }

    /**
     * Retrieve metadata for the given object.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return
     */
    public StorageObject getMetadata(StorageLocation storageLocation, String path) throws StorageObjectNotFoundException {
        String relativePath = relativizePath(storageLocation, path);
        LOG.info("Getting {} from location for {}", relativePath, storageLocation.getPathPrefix());
        return storageService.listStorageContent(storageLocation, relativePath, 1, subjectKey, authToken)
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
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return list of child objects
     */
    public List<StorageObject> getChildren(StorageLocation storageLocation, String path) throws StorageObjectNotFoundException {
        String relativePath = relativizePath(storageLocation, path);
        return getDescendants(storageLocation, relativePath, 1);
    }

    /**
     * List the child objects of the given storage object. If the given object doesn't exist, then an
     * IllegalArgumentException will be thrown. If the given object is not a collection, then an empty list will be
     * returned.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return list of descendant objects
     */
    public List<StorageObject> getDescendants(StorageLocation storageLocation, String path, int depth) throws StorageObjectNotFoundException {
        String relativePath = relativizePath(storageLocation, path);
        return storageService.listStorageContent(storageLocation, relativePath, depth, subjectKey, authToken)
                .stream()
                // We're only interested in descendants, so filter out the blank relative path which represents the
                // root object, or any path which is the same as the path we asked for.
                .filter(c -> StringUtils.isNotBlank(c.getObjectName()) && !c.getRelativePath().equals(StringUtils.appendIfMissing(relativePath, "/")))
                .collect(Collectors.toList());
    }

    /**
     * Return whether or not the given object exists in storage.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return true if the path exists
     */
    public boolean exists(StorageLocation storageLocation, String path) {
        String relativePath = relativizePath(storageLocation, path);
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("exists requesting "+contentURL);
        return storageService.exists(contentURL, subjectKey, authToken);
    }

    /**
     * Returns the content of the given object. The object should be a file, not a collection.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return stream of the content in the given object
     */
    public InputStream getContent(StorageLocation storageLocation, String path) {
        String relativePath = relativizePath(storageLocation, path);
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("getContent requesting "+contentURL);
        return storageService.getStorageContent(contentURL, subjectKey, authToken);
    }

    /**
     * Returns the content of the given object as a UTF-8 string. The object should be a file, not a collection.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return stream of the content in the given object
     */
    public String getContentAsString(StorageLocation storageLocation, String path) throws IOException {
        String relativePath = relativizePath(storageLocation, path);
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("getContentAsString requesting "+contentURL);
        InputStream stream = storageService.getStorageContent(contentURL, subjectKey, authToken);
        return IOUtils.toString(stream, StandardCharsets.UTF_8);
    }

    /**
     * Returns the size in bytes of the given object.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return stream of the content in the given object
     */
    public long getContentLength(StorageLocation storageLocation, String path) {
        String relativePath = relativizePath(storageLocation, path);
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("getContentLength requesting "+contentURL);
        return  storageService.getContentLength(contentURL, subjectKey, authToken);
    }

    /**
     * TODO: this doesn't work yet because it turns out that N5TreeNode is not serializable
     * Discover n5 data sets and return a tree of N5 objects metadata for the given object.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return tree of data sets represented by N5TreeNode
     */
    public N5TreeNode getN5Tree(StorageLocation storageLocation, String path) throws StorageObjectNotFoundException {
        String relativePath = relativizePath(storageLocation, path);
        Client httpclient = HttpUtils.createHttpClient();
        String storageURL = storageLocation.getStorageURL();
        try {
            WebTarget target = httpclient.target(storageURL).path("n5tree");
            if (StringUtils.isNotBlank(relativePath)) {
                target = target.path(relativePath);
            }
            else {
                target = target.path("/");
            }
            LOG.debug("getN5Tree requesting {}", target.getUri().toString());
            Invocation.Builder requestBuilder = storageService.createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subjectKey, authToken);
            Response response = requestBuilder.get();
            if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new StorageObjectNotFoundException(storageLocation, relativePath);
            }
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
            return response.readEntity(new GenericType<N5TreeNode>(){});
        }
        finally {
            httpclient.close();
        }
    }

    /**
     * Given a path inside of a storage location, remove the part of the path provided by the storage location,
     * if the path is absolute. If the path is already relative, return it.
     * @param storageLocation location of an object
     * @param path path within the storage location
     * @return path relative to the storage location
     */
    private String relativizePath(StorageLocation storageLocation, String path) {
        if (StringUtils.isBlank(path)) return "";
        if (path.charAt(0)=='/') {
            // This is an absolute path
            return storageLocation.getRelativePath(path);
        }
        return path;
    }
}
