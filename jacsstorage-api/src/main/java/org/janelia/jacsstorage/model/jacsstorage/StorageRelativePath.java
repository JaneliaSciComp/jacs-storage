package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class StorageRelativePath {

    enum StorageRelativePathType {
        relativeToVirtualRoot,
        relativeToBaseRoot
    }

    public static StorageRelativePath pathRelativeToBaseRoot(String storageRelativePath) {
        return new StorageRelativePath(storageRelativePath, StorageRelativePathType.relativeToBaseRoot);
    }

    public static StorageRelativePath pathRelativeToVirtualRoot(String storageRelativePath) {
        return new StorageRelativePath(storageRelativePath, StorageRelativePathType.relativeToVirtualRoot);
    }

    private final String path;
    private final StorageRelativePathType pathType;

    private StorageRelativePath(String path, StorageRelativePathType pathType) {
        this.path = path;
        this.pathType = pathType;
    }

    boolean isRelativeToBaseRoot() {
        return pathType == StorageRelativePathType.relativeToBaseRoot;
    }

    boolean isRelativeToVirtualRoot() {
        return pathType == StorageRelativePathType.relativeToVirtualRoot;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("path", path)
                .append("pathType", pathType)
                .toString();
    }
}
