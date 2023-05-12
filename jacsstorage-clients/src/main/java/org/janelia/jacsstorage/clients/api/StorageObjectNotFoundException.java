package org.janelia.jacsstorage.clients.api;

/**
 * Exception when a storage location is requested by the user but it cannot be found in JADE at the given
 * StorageLocation.
 */
public class StorageObjectNotFoundException extends Exception {

    private StorageLocation storageLocation;
    private String relativePath;

    public StorageObjectNotFoundException(StorageLocation storageLocation, String relativePath) {
        super("Could not find "+storageLocation.getStorageURLForRelativePath(relativePath));
        this.storageLocation = storageLocation;
        this.relativePath = relativePath;
    }

    public StorageLocation getStorageLocation() {
        return storageLocation;
    }

    public String getRelativePath() {
        return relativePath;
    }
}
