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
     * @param storageURIDesc
     * @return
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
    public String getStorageKey() {
        if (getStorageType() == JacsStorageType.FILE_SYSTEM) {
            StringBuilder storageKeyBuilder = new StringBuilder();
            if (StringUtils.isNotBlank(getStorageHost())) {
                storageKeyBuilder.append('/').append(getStorageHost());
            }
            storageKeyBuilder.append(storageURI.getPath());
            return storageKeyBuilder.toString();
        } else {
            return StringUtils.removeStart(storageURI.getPath(), '/');
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
        return StringUtils.isEmpty(getStorageKey());
    }

    public String getJadeStorage() {
        return resolveJadeStorage(null);
    }

    public String resolveJadeStorage(String relativePath) {
        StringBuilder jadeStorageBuilder = new StringBuilder();
        if (getStorageType() == JacsStorageType.FILE_SYSTEM) {
            jadeStorageBuilder.append(getStorageKey());
        } else {
            jadeStorageBuilder.append(getStorageScheme().value).append("://");
            if (StringUtils.isNotBlank(getUserAccessKey())) {
                jadeStorageBuilder.append(getUserAccessKey());
                if (StringUtils.isNotBlank(getUserSecretKey())) {
                    jadeStorageBuilder.append(':').append(getUserSecretKey());
                }
                jadeStorageBuilder.append('@');
            }
            jadeStorageBuilder.append(getStorageHost());
            String storageKey = getStorageKey();
            if (StringUtils.isNotBlank(storageKey)) {
                // s3 keys should not start with a '/'
                jadeStorageBuilder.append('/').append(storageKey);
            }
        }
        String appendedPath = StringUtils.removeStart(relativePath, '/');
        if (StringUtils.isNotBlank(appendedPath)) {
            jadeStorageBuilder.append('/').append(appendedPath);
        }
        return jadeStorageBuilder.toString();
    }

    public @Nullable String relativize(@Nonnull JADEStorageURI otherStorageURI) {
        if (this.getStorageHost().equals(otherStorageURI.getStorageHost())) {
            String thisStorageKey = this.getStorageKey();
            String otherStorageKey = otherStorageURI.getStorageKey();
            if (!StringUtils.startsWith(otherStorageKey, thisStorageKey)) {
                // I consider this viable only if otherStorage key starts with current's storage key
                return null;
            }
            return Paths.get(thisStorageKey).relativize(Paths.get(otherStorageURI.getStorageKey())).toString();
        } else {
            return null;
        }
    }

    public @Nullable String relativizeKey(@Nonnull String otherContentKey) {
        String thisStorageKey = this.getStorageKey();
        if (!StringUtils.startsWith(otherContentKey, thisStorageKey)) {
            // I consider this viable only if otherContent key starts with current's storage key
            return null;
        }
        return Paths.get(thisStorageKey).relativize(Paths.get(otherContentKey)).toString();
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
