package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.StringUtils;

public enum JacsStorageType {
    FILE_SYSTEM, S3;

    public static JacsStorageType fromName(String storageType) {
        if (StringUtils.equalsIgnoreCase(storageType, "s3")) {
            return  S3;
        } else if (StringUtils.isBlank(storageType) ||
                StringUtils.equalsIgnoreCase(storageType, "file") ||
                StringUtils.equalsIgnoreCase(storageType, "file_system")) {
            return FILE_SYSTEM;
        } else {
            throw new IllegalArgumentException("Invalid storage type: " + storageType);
        }
    }
}
