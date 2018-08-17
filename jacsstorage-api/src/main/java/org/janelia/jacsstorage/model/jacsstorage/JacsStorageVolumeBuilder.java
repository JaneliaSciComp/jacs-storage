package org.janelia.jacsstorage.model.jacsstorage;

import java.util.Set;

public class JacsStorageVolumeBuilder {

    private JacsStorageVolume jacsStorageVolume = new JacsStorageVolume();

    public JacsStorageVolume build() {
        JacsStorageVolume toReturn = jacsStorageVolume;
        jacsStorageVolume = new JacsStorageVolume();
        return toReturn;
    }

    public JacsStorageVolumeBuilder storageVolumeId(Number v) {
        jacsStorageVolume.setId(v);
        return this;
    }

    public JacsStorageVolumeBuilder storageHost(String v) {
        jacsStorageVolume.setStorageHost(v);
        return this;
    }

    public JacsStorageVolumeBuilder name(String v) {
        jacsStorageVolume.setName(v);
        return this;
    }

    public JacsStorageVolumeBuilder storageVirtualPath(String v) {
        jacsStorageVolume.setStorageVirtualPath(v);
        return this;
    }

    public JacsStorageVolumeBuilder storageRootTemplate(String v) {
        jacsStorageVolume.setStorageRootTemplate(v);
        return this;
    }

    public JacsStorageVolumeBuilder addTag(String v) {
        jacsStorageVolume.addStorageTag(v);
        return this;
    }

    public JacsStorageVolumeBuilder storageServiceURL(String v) {
        jacsStorageVolume.setStorageServiceURL(v);
        return this;
    }

    public JacsStorageVolumeBuilder availableSpace(Long v) {
        jacsStorageVolume.setAvailableSpaceInBytes(v);
        return this;
    }

    public JacsStorageVolumeBuilder percentageFull(Integer v) {
        jacsStorageVolume.setPercentageFull(v);
        return this;
    }

    public JacsStorageVolumeBuilder shared(boolean flag) {
        jacsStorageVolume.setShared(flag);
        return this;
    }

    public JacsStorageVolumeBuilder volumePermissions(Set<JacsStoragePermission> permissions) {
        jacsStorageVolume.setVolumePermissions(permissions);
        return this;
    }

}
