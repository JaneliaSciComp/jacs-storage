package org.janelia.jacsstorage.model.jacsstorage;

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

    public JacsBundleBuilder owner(String v) {
        jacsBundle.setOwner(v);
        return this;
    }

    public JacsBundleBuilder location(String v) {
        jacsBundle.setStorageVolume(jacsBundle.getStorageVolume().map(sv -> {
            sv.setLocation(v);
            return sv;
        }).orElseGet(() -> {
            JacsStorageVolume sv = new JacsStorageVolume();
            sv.setLocation(v);
            return sv;
        }));
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
}
