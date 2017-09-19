package org.janelia.jacsstorage.rest;

import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataStorageRequest {
    private String name;
    private String owner;
    private String permissions;
    private JacsStorageFormat storageFormat;
    private Long requestedSpaceInKB;
    private Map<String, Object> metadata = new LinkedHashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getPermissions() {
        return permissions;
    }

    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }

    public JacsStorageFormat getStorageFormat() {
        return storageFormat;
    }

    public void setStorageFormat(JacsStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
    }

    public Long getRequestedSpaceInKB() {
        return requestedSpaceInKB;
    }

    public void setRequestedSpaceInKB(Long requestedSpaceInKB) {
        this.requestedSpaceInKB = requestedSpaceInKB;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public JacsBundle asDataBundle() {
        JacsBundle dataBundle = new JacsBundle();
        dataBundle.setName(this.name);
        dataBundle.setOwner(this.owner);
        dataBundle.setPermissions(this.permissions);
        dataBundle.setUsedSpaceInKB(this.requestedSpaceInKB);
        dataBundle.addMetadataFields(this.metadata);
        return dataBundle;
    }
}
