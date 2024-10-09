package org.janelia.jacsstorage.model.jacsstorage;

import java.util.HashMap;
import java.util.Map;

public class JADEStorageOptions {
    private Map<String, Object> options = new HashMap<>();

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
        return (String) options.getOrDefault("accessKey", defaultAccessKey);
    }

    public JADEStorageOptions setAccessKey(String accessKey) {
        if (accessKey != null) {
            options.put("accessKey", accessKey);
        } else {
            options.remove("accessKey");
        }
        return this;
    }

    public String getSecretKey(String defaultSecretKey) {
        return (String) options.getOrDefault("secretKey", defaultSecretKey);
    }

    public JADEStorageOptions setSecretKey(String secretKey) {
        if (secretKey != null) {
            options.put("secretKey", secretKey);
        } else {
            options.remove("secretKey");
        }
        return this;
    }

    public String getRegion(String defaultRegion) {
        return (String) options.getOrDefault("region", defaultRegion);
    }

    public JADEStorageOptions setRegion(String region) {
        if (region != null) {
            options.put("region", region);
        } else {
            options.remove("region");
        }
        return this;
    }
}
