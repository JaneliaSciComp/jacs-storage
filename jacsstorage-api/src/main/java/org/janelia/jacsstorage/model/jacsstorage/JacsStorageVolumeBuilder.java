package org.janelia.jacsstorage.model.jacsstorage;

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

    public JacsStorageVolumeBuilder storagePathPrefix(String v) {
        jacsStorageVolume.setStoragePathPrefix(v);
        return this;
    }

    public JacsStorageVolumeBuilder storageRootDir(String v) {
        jacsStorageVolume.setStorageRootDir(v);
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
}
