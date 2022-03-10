package org.janelia.jacsstorage.newclient;

public class StorageContentObject extends StorageObject {

    private StorageContentInfo contentInfo;
    private Boolean isCollection;

    public StorageContentObject(BetterStorageHelper helper, StorageLocation location,
                                StorageObject parent, StorageContentInfo contentInfo) {
        super(helper, location, parent, contentInfo.getRemoteInfo().getEntryRelativePath());
        this.contentInfo = contentInfo;
        this.isCollection = contentInfo.getRemoteInfo().isCollection();
    }

    /**
     * Does this object represent a directory which has contents that can be listed?
     * @return true if the object is a directory and can be listed
     */
    public boolean isCollection() {
        if (isCollection == null) {
            throw new IllegalStateException("Cannot call isCollection on a StorageObject which doesn't have StorageContentInfo");
        }
        return isCollection;
    }

    /**
     * Return the name of the object, without any path information.
     * @return name of the object
     */
    public String getName() {
        // Deal with inconsistency in the Jade API:
        if (contentInfo.getRemoteInfo().isCollection()) {
            // If you list a directory then the nodeRelativePaths are relative to the path you listed,
            return contentInfo.getRemoteInfo().getEntryRelativePath();
        }
        else {
            // But if you list a file, then the nodeRelativePath is relative to the volume, so we need to have an empty name here
            return "";
        }
    }
}
