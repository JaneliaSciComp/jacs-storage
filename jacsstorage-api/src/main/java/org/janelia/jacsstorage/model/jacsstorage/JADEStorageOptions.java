package org.janelia.jacsstorage.model.jacsstorage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class JADEStorageOptions {
    private Map<String, Object> options = new HashMap<>();

    public Collection<String> getAttributeNames() {
        return options.keySet();
    }

    public String getAsString(String key, String defaultValue) {
        return (String) options.getOrDefault(key, defaultValue);
    }

    public JADEStorageOptions setAsString(String key, String value) {
        if (value != null) {
            options.put(key, value);
        } else {
            options.remove(key);
        }
        return this;
    }

    public String getAccessKey(String defaultAccessKey) {
        return (String) options.getOrDefault("AccessKey", defaultAccessKey);
    }

    public JADEStorageOptions setAccessKey(String accessKey) {
        if (accessKey != null) {
            options.put("AccessKey", accessKey);
        } else {
            options.remove("AccessKey");
        }
        return this;
    }

    public String getSecretKey(String defaultSecretKey) {
        return (String) options.getOrDefault("SecretKey", defaultSecretKey);
    }

    public JADEStorageOptions setSecretKey(String secretKey) {
        if (secretKey != null) {
            options.put("SecretKey", secretKey);
        } else {
            options.remove("SecretKey");
        }
        return this;
    }

    public String getRegion(String defaultRegion) {
        return (String) options.getOrDefault("Region", defaultRegion);
    }

    public JADEStorageOptions setRegion(String region) {
        if (region != null) {
            options.put("Region", region);
        } else {
            options.remove("Region");
        }
        return this;
    }
}
