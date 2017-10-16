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

    public JacsStorageVolumeBuilder name(String v) {
        jacsStorageVolume.setName(v);
        return this;
    }

    public JacsStorageVolumeBuilder location(String v) {
        jacsStorageVolume.setLocation(v);
        return this;
    }

    public JacsStorageVolumeBuilder mountHostIP(String v) {
        jacsStorageVolume.setMountHostIP(v);
        return this;
    }

    public JacsStorageVolumeBuilder mountHostURL(String v) {
        jacsStorageVolume.setMountHostURL(v);
        return this;
    }

    public JacsStorageVolumeBuilder mountPoint(String v) {
        jacsStorageVolume.setMountPoint(v);
        return this;
    }
}
