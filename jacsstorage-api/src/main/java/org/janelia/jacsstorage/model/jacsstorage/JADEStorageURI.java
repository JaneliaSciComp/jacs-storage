package org.janelia.jacsstorage.model.jacsstorage;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;

public class JADEStorageURI {
    private static final String JADE_URI_SCHEME = "jade://";

    /**
     * Creates a StoragePathURI taking the path value exactly as is.
     * @param storageURIDesc
     * @return
     */
    public static JADEStorageURI createStoragePathURI(String storageURIDesc) {
        return new JADEStorageURI(URI.create(normalizeURIDesc(storageURIDesc)));
    }

    private static String normalizeURIDesc(String uriDesc) {
        if (StringUtils.isBlank(uriDesc)) {
            return "";
        }
        if (StringUtils.startsWithIgnoreCase(uriDesc, JADE_URI_SCHEME)) {
            String unprefixedURIDesc = uriDesc.substring(JADE_URI_SCHEME.length());
            if (unprefixedURIDesc.matches("^(\\w+://).*")) {
                return unprefixedURIDesc;
            } else {
                return uriDesc;
            }
        } else {
            return uriDesc;
        }
    }

    private final URI storageURI;

    private JADEStorageURI(URI storageURI) {
        this.storageURI = storageURI;
    }

    public URI getStorageURI() {
        return storageURI;
    }

    public JacsStorageType getStorageType() {
        String storageScheme = StringUtils.isBlank(storageURI.getScheme()) ? "" : storageURI.getScheme().toLowerCase();
        if (storageScheme.equals("s3") ||
                storageScheme.equals("https") ||
                StringUtils.isNotBlank(getStorageHost())) {
            return JacsStorageType.S3;
        } else {
            return JacsStorageType.FILE_SYSTEM;
        }
    }

    public String getStorageHost() {
        return storageURI.getHost() == null ? "" : storageURI.getHost();
    }

    public String getStorageKey() {
        return storageURI.getPath();
    }

    public String getUserAccessKey() {
        if (StringUtils.isNotBlank(storageURI.getUserInfo())) {
            String accessKeyAndSecret = storageURI.getUserInfo();
            int separatorIndex = accessKeyAndSecret.indexOf(":");
            return separatorIndex == -1 ? accessKeyAndSecret : accessKeyAndSecret.substring(0, separatorIndex);
        } else {
            return "";
        }
    }

    public String getUserSecretKey() {
        if (StringUtils.isNotBlank(storageURI.getUserInfo())) {
            String accessKeyAndSecret = storageURI.getUserInfo();
            int separatorIndex = accessKeyAndSecret.indexOf(":");
            return separatorIndex == -1 ? "" : accessKeyAndSecret.substring(separatorIndex+1);
        } else {
            return "";
        }
    }

    @Override
    public String toString() {
        return storageURI.toString();
    }
}
