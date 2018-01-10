package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class JacsBundleBuilder {

    private JacsBundle jacsBundle = new JacsBundle();

    public JacsBundle build() {
        JacsBundle toReturn = jacsBundle;
        jacsBundle = new JacsBundle();
        return toReturn;
    }

    public JacsBundleBuilder dataBundleId(Number v) {
        jacsBundle.setId(v);
        return this;
    }

    public JacsBundleBuilder name(String v) {
        jacsBundle.setName(v);
        return this;
    }

    public JacsBundleBuilder ownerKey(String v) {
        jacsBundle.setOwnerKey(v);
        return this;
    }

    public JacsBundleBuilder path(String v) {
        jacsBundle.setPath(v);
        return this;
    }

    public JacsBundleBuilder storageFormat(JacsStorageFormat v) {
        jacsBundle.setStorageFormat(v);
        return this;
    }

    public JacsBundleBuilder storagePathPrefix(String v) {
        updateBundleStorageVolume(sv -> {
            sv.setStoragePathPrefix(v);
        });
        return this;
    }

    public JacsBundleBuilder storageTags(String tags) {
        if (StringUtils.isNotBlank(tags)) {
            return storageTags(Arrays.asList(tags.split(",")));
        }
        return this;
    }

    private JacsBundleBuilder storageTags(List<String> tags) {
        updateBundleStorageVolume(sv -> {
            tags.stream()
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim)
                    .forEach(sv::addStorageTag);
        });
        return this;
    }

    public JacsBundleBuilder storageHost(String v) {
        updateBundleStorageVolume(sv -> {
            sv.setStorageHost(v);
        });
        return this;
    }

    public JacsBundleBuilder volumeName(String v) {
        updateBundleStorageVolume(sv -> {
            sv.setName(v);
        });
        return this;
    }

    public JacsBundleBuilder usedSpaceInBytes(Long v) {
        jacsBundle.setUsedSpaceInBytes(v);
        return this;
    }

    public JacsBundleBuilder checksum(String v) {
        jacsBundle.setChecksum(v);
        return this;
    }

    public JacsBundleBuilder storageVolumeId(Number v) {
        jacsBundle.setStorageVolumeId(v);
        return this;
    }

    private Optional<JacsStorageVolume> updateBundleStorageVolume(Consumer<JacsStorageVolume> storageVolumeUpdater) {
        return jacsBundle.setStorageVolume(jacsBundle.getStorageVolume().orElse(new JacsStorageVolume()))
                .map(sv -> {
                    storageVolumeUpdater.accept(sv);
                    return sv;
                });
    }
}
