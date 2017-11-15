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

    public JacsBundleBuilder path(String v) {
        jacsBundle.setPath(v);
        return this;
    }

    public JacsBundleBuilder storageFormat(JacsStorageFormat v) {
        jacsBundle.setStorageFormat(v);
        return this;
    }

    public JacsBundleBuilder storageHost(String v) {
        jacsBundle.setStorageVolume(jacsBundle.getStorageVolume().map(sv -> {
            sv.setStorageHost(v);
            return sv;
        }).orElseGet(() -> {
            JacsStorageVolume sv = new JacsStorageVolume();
            sv.setStorageHost(v);
            return sv;
        }));
        return this;
    }

    public JacsBundleBuilder volumeName(String v) {
        jacsBundle.setStorageVolume(jacsBundle.getStorageVolume().map(sv -> {
            sv.setName(v);
            return sv;
        }).orElseGet(() -> {
            JacsStorageVolume sv = new JacsStorageVolume();
            sv.setName(v);
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
