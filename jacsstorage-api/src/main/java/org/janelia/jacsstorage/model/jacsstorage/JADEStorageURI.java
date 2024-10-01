package org.janelia.jacsstorage.model.jacsstorage;

import java.net.URI;
import java.nio.file.Paths;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class JADEStorageURI {
    private static final String JADE_URI_SCHEME = "jade://";

    public enum JADEStorageScheme {
        NONE(""),
        HTTP("https"),
        S3("s3");

        private final String value;

        JADEStorageScheme(String value) {
            this.value = value;
        }
    }

    /**
     * Creates a StoragePathURI taking the path value exactly as is.
     *
     * @param storageURIDesc source URI descriptor
     * @return create a JADEStorageURI from the given URI descriptor
     */
    public static JADEStorageURI createStoragePathURI(String storageURIDesc) {
        return new JADEStorageURI(URI.create(normalizeURIDesc(storageURIDesc).replace("%", "%25")));
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
            if (uriDesc.matches("^(\\w+://).*")) {
                // if a scheme is present just return it as is
                // but the caller must be careful with 'file://' URIs if they don't reference an absolute path
                return uriDesc;
            } else {
                // if there's no URI scheme make sure the path is absolute
                return uriDesc.charAt(0) == '/' ? uriDesc : "/" + uriDesc;
            }
        }
    }

    private final URI storageURI;

    private JADEStorageURI(URI storageURI) {
        this.storageURI = storageURI;
    }

    public JacsStorageType getStorageType() {
        if (StringUtils.isNotBlank(storageURI.getHost())) {
            if (StringUtils.equalsIgnoreCase(storageURI.getScheme(), "file") ||
                    StringUtils.equalsIgnoreCase(storageURI.getScheme(), "jade")) {
                return JacsStorageType.FILE_SYSTEM;
            } else {
                return JacsStorageType.S3;
            }
        } else {
            return JacsStorageType.FILE_SYSTEM;
        }
    }

    public JADEStorageScheme getStorageScheme() {
        if (getStorageType() == JacsStorageType.S3) {
            if (StringUtils.equalsIgnoreCase(storageURI.getScheme(), "s3")) {
                return JADEStorageScheme.S3;
            } else {
                return JADEStorageScheme.HTTP;
            }
        } else {
            return JADEStorageScheme.NONE;
        }
    }

    public String getStorageHost() {
        return storageURI.getHost() == null ? "" : storageURI.getHost();
    }

    public String getStorageEndpoint() {
        return storageURI.getHost() == null ? "" : getStorageScheme().value + "://" + storageURI.getHost();
    }

    /**
     * @return stored object's path.
     * So for FileSystem storage it should return file's path,
     * for S3 it should return object's key
     */
    public String getJADEKey() {
        if (getStorageType() == JacsStorageType.FILE_SYSTEM) {
            return getFileSystemContentKey();
        } else if (getStorageType() == JacsStorageType.S3) {
            return StringUtils.removeEnd(storageURI.getPath(), "/");
        } else {
            return null;
        }
    }

    /**
     * @return content's key using the following rules:
     * for the file system the content key is the same as the full path
     * for S3
     * if the scheme is S3 the content key is the same as the one parsed by the URI parser
     * if the scheme is HTTPS we assume the first component in the path is the bucket which will
     * not be part of the content key
     */
    public String getContentKey() {
        if (getStorageType() == JacsStorageType.FILE_SYSTEM) {
            return getFileSystemContentKey();
        } else if (getStorageType() == JacsStorageType.S3) {
            return getS3ContentKey();
        } else {
            return null;
        }
    }

    private String getFileSystemContentKey() {
        StringBuilder storageKeyBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(getStorageHost())) {
            storageKeyBuilder.append('/').append(getStorageHost());
        }
        storageKeyBuilder.append(storageURI.getPath());
        return storageKeyBuilder.toString();
    }

    private String getS3ContentKey() {
        String storagePath = StringUtils.removeStart(storageURI.getPath(), '/');
        if (getStorageScheme() == JADEStorageScheme.S3) {
            return StringUtils.removeStart(storagePath, '/');
        } else {
            int pathSeparator = storagePath.indexOf('/');
            if (pathSeparator == -1) {
                return "";
            } else {
                return storagePath.substring(pathSeparator + 1);
            }
        }
    }

    public String getContentBucket() {
        if (getStorageType() == JacsStorageType.FILE_SYSTEM) {
            return "";
        } else if (getStorageType() == JacsStorageType.S3) {
            if (getStorageScheme() == JADEStorageScheme.S3) {
                return storageURI.getHost();
            } else {
                String bucketName = extractBucketNameFromPath();
                if (StringUtils.isEmpty(bucketName)) {
                    return extractBucketNameFromHost();
                } else {
                    return bucketName;
                }
            }
        } else {
            return null;
        }
    }

    private String extractBucketNameFromPath() {
        String storagePath = StringUtils.removeStart(storageURI.getPath(), '/');
        if (StringUtils.isEmpty(storagePath)) {
            return null;
        } else {
            int pathSeparator = storagePath.indexOf('/');
            if (pathSeparator == -1) {
                return storagePath;
            } else {
                return storagePath.substring(0, pathSeparator);
            }
        }
    }

    private String extractBucketNameFromHost() {
        String host = getStorageHost();
        if (StringUtils.isEmpty(host)) {
            return null;
        } else {
            int domainSeparator = host.indexOf('.');
            if (domainSeparator == -1) {
                return host;
            } else {
                return host.substring(0, domainSeparator);
            }
        }
    }

    /**
     * @return stored object's file name.
     */
    public String getObjectName() {
        return Paths.get(storageURI.getPath()).getFileName().toString();
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
            return separatorIndex == -1 ? "" : accessKeyAndSecret.substring(separatorIndex + 1);
        } else {
            return "";
        }
    }

    public boolean isEmpty() {
        return StringUtils.isEmpty(storageURI.getHost()) && StringUtils.isEmpty(storageURI.getPath());
    }

    /**
     * @return the stringified JADEStorageURI which for HTTP scheme will include the bucket name in the path.
     */
    public String getJadeStorage() {
        return resolveJadeStorage(null);
    }

    public String resolveJadeStorage(String relativePath) {
        StringBuilder jadeStorageBuilder = new StringBuilder();
        if (getStorageType() == JacsStorageType.FILE_SYSTEM) {
            jadeStorageBuilder.append(getFileSystemContentKey());
        } else if (getStorageType() == JacsStorageType.S3) {
            jadeStorageBuilder.append(getStorageScheme().value).append("://");
            if (StringUtils.isNotBlank(getUserAccessKey())) {
                jadeStorageBuilder.append(getUserAccessKey());
                if (StringUtils.isNotBlank(getUserSecretKey())) {
                    jadeStorageBuilder.append(':').append(getUserSecretKey());
                }
                jadeStorageBuilder.append('@');
            }
            jadeStorageBuilder
                    .append(getStorageHost())
                    .append(getJADEKey());
        } else {
            throw new IllegalStateException("Method cannot be invoked when the storage type is unknow");
        }
        String appendedPath = StringUtils.removeStart(relativePath, '/');
        if (StringUtils.isNotBlank(appendedPath)) {
            jadeStorageBuilder.append('/').append(appendedPath);
        }
        return jadeStorageBuilder.toString();
    }

    public @Nullable String relativize(@Nonnull JADEStorageURI otherStorageURI) {
        if (this.getStorageType() == JacsStorageType.FILE_SYSTEM) {
            return Paths.get(this.getFileSystemContentKey()).relativize(Paths.get(otherStorageURI.getFileSystemContentKey())).toString();
        } else if (this.getStorageType() == JacsStorageType.S3) {
            if (StringUtils.equalsIgnoreCase(this.getContentBucket(), otherStorageURI.getContentBucket())) {
                // the URIs must have the same bucket
                String thisContentKey = this.getContentKey();
                String otherContentKey = otherStorageURI.getContentKey();
                if (!StringUtils.startsWith(otherContentKey, thisContentKey)) {
                    // I consider this viable only if otherStorage key starts with current's storage key
                    return null;
                } else {
                    return otherContentKey.substring(thisContentKey.length());
                }
            }
            return null;
        } else {
            throw new IllegalStateException("Method cannot be invoked when the storage type is unknow");
        }
    }

    public @Nullable String relativizeKey(@Nonnull String otherContentKey) {
        if (this.getStorageType() == JacsStorageType.FILE_SYSTEM) {
            return Paths.get(this.getFileSystemContentKey()).relativize(Paths.get(otherContentKey)).toString();
        } else if (this.getStorageType() == JacsStorageType.S3) {
            String thisContentKey = this.getContentKey();
            if (!StringUtils.startsWith(otherContentKey, thisContentKey)) {
                // I consider this viable only if otherStorage key starts with current's storage key
                return null;
            } else {
                return otherContentKey.substring(thisContentKey.length());
            }
        } else {
            throw new IllegalStateException("Method cannot be invoked when the storage type is unknow");
        }
    }

    public JADEStorageURI resolve(String relativePath) {
        return JADEStorageURI.createStoragePathURI(resolveJadeStorage(relativePath));
    }

    @Override
    public String toString() {
        return getJadeStorage();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        JADEStorageURI that = (JADEStorageURI) o;

        return new EqualsBuilder()
                .append(getJadeStorage(), that.getJadeStorage())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getJadeStorage())
                .toHashCode();
    }
}
