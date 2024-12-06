package org.janelia.jacsstorage.clients.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.saalfeldlab.n5.universe.N5TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client for JADE which exposes the service as a file-like API.
 *
 * Builds on top of StorageService to create a better storage API by abstracting away JADE implementation details
 * and letting the user deal with JADE as a file object storage service.
 */
public class JadeStorageService {

    private static final Logger LOG = LoggerFactory.getLogger(JadeStorageService.class);

    private final JadeHttpClient jadeHttpClient;

    private final String subjectKey;
    private final String authToken;

    public JadeStorageService(String masterStorageServiceURL, String storageServiceApiKey) {
        this(masterStorageServiceURL, storageServiceApiKey, null, null);
    }

    public JadeStorageService(String masterStorageServiceURL, String storageServiceApiKey, String subjectKey, String authToken) {
        this.jadeHttpClient = new JadeHttpClient(masterStorageServiceURL, storageServiceApiKey);
        this.subjectKey = subjectKey;
        this.authToken = authToken;
    }

    /**
     * Find the StorageLocation of the given path. If no storage location exists, null is returned.
     * @param path full JADE path to locate
     * @return
     */
    public StorageLocation getStorageLocationByPath(String path, JadeStorageAttributes storageOptions) {
        return jadeHttpClient.lookupStorage(path, subjectKey, authToken, storageOptions).orElse(null);
    }

    /**
     * Retrieve metadata for the given object.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @param dirsOnly if true only get subdirectories
     * @return
     */
    public StorageObject getMetadata(StorageLocation storageLocation, String path, boolean dirsOnly) throws StorageObjectNotFoundException {
        String relativePath = relativizePath(storageLocation, path);
        LOG.info("Getting {} from location for {}", relativePath, storageLocation.getPathPrefix());
        return jadeHttpClient.listStorageContent(storageLocation, relativePath, 1, dirsOnly, subjectKey, authToken)
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
    public List<StorageObject> getChildren(StorageLocation storageLocation, String path, boolean dirsOnly) throws StorageObjectNotFoundException {
        String relativePath = relativizePath(storageLocation, path);
        return getDescendants(storageLocation, relativePath, 1, dirsOnly);
    }

    /**
     * List the child objects of the given storage object. If the given object doesn't exist, then an
     * IllegalArgumentException will be thrown. If the given object is not a collection, then an empty list will be
     * returned.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @param dirsOnly if set get only subdirs
     * @return list of descendant objects
     */
    public List<StorageObject> getDescendants(StorageLocation storageLocation, String path, int depth, boolean dirsOnly) throws StorageObjectNotFoundException {
        String relativePath = relativizePath(storageLocation, path);
        return jadeHttpClient.listStorageContent(storageLocation, relativePath, depth, dirsOnly, subjectKey, authToken)
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
        return jadeHttpClient.exists(contentURL, subjectKey, authToken, storageLocation.getStorageAttributes());
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
        return jadeHttpClient.getStorageContent(contentURL, subjectKey, authToken, storageLocation.getStorageAttributes());
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
        InputStream stream = jadeHttpClient.getStorageContent(contentURL, subjectKey, authToken, storageLocation.getStorageAttributes());
        return IOUtils.toString(stream, StandardCharsets.UTF_8);
    }

    /**
     * Sets the content of the given object. The object should be a file, not a collection.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @param inputStream stream of the content in the given object
     */
    public void setContent(StorageLocation storageLocation, String path, InputStream inputStream) {
        String relativePath = relativizePath(storageLocation, path);
        String contentURL = storageLocation.getStorageURLForRelativePath(relativePath);
        LOG.debug("setContent for "+contentURL);
        jadeHttpClient.setStorageContent(contentURL, subjectKey, authToken, storageLocation.getStorageAttributes(), inputStream);
    }

    /**
     * TODO: this doesn't work yet because it turns out that N5TreeNode is not serializable
     * Discover n5 data sets and return a tree of N5 objects metadata for the given object.
     * @param storageLocation location of the object
     * @param path path to the object, either absolute or relative to the storageLocation
     * @return tree of data sets represented by N5TreeNode
     */
    public N5TreeNode getN5Tree(StorageLocation storageLocation, String path) throws StorageObjectNotFoundException {
        throw new UnsupportedOperationException("This isn't supported yet");
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
