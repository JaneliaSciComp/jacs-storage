package org.janelia.jacsstorage.model.jacsstorage;

import java.util.HashMap;
import java.util.Map;

public class JADEOptions {

    public static JADEOptions create() {
        return new JADEOptions(new HashMap<>());
    }

    public static JADEOptions createFromOptions(JADEOptions jadeOptions) {
        return new JADEOptions(jadeOptions.options);
    }

    private final Map<String, Object> options;

    private JADEOptions(Map<String, Object> options) {
        this.options = options;
    }

    public String getAsString(String key) {
        return (String) options.get(key);
    }

    public Boolean getAsBoolean(String key) {
        return (Boolean) options.get(key);
    }

    public Object get(String key) {
        return options.get(key);
    }

    private JADEOptions set(String key, Object value) {
        if (value != null) {
            options.put(key, value);
        } else {
            options.remove(key);
        }
        return this;
    }

    private JADEOptions setIfMissing(String key, Object value) {
        if (value != null) {
            options.putIfAbsent(key, value);
        }
        return this;
    }

    public String getAccessKey() {
        return getAsString("AccessKey");
    }

    public JADEOptions setAccessKey(String accessKey) {
        return set("AccessKey", accessKey);
    }

    public String getSecretKey() {
        return getAsString("SecretKey");
    }

    public JADEOptions setSecretKey(String secretKey) {
        return set("SecretKey", secretKey);
    }

    public String getAWSRegion() {
        return getAsString("AWSRegion");
    }

    public JADEOptions setAWSRegion(String region) {
        return set("AWSRegion", region);
    }

    public JADEOptions setDefaultAWSRegion(String region) {
        return setIfMissing("AWSRegion", region);
    }

    public Boolean getPathStyleBucket() {
        return getAsBoolean("PathStyleBucket");
    }

    public JADEOptions setPathStyleBucket(Boolean pathStyleBucket) {
        return set("PathStyleBucket", pathStyleBucket);
    }

    public JADEOptions setDefaultPathStyleBucket(Boolean pathStyleBucket) {
        return setIfMissing("PathStyleBucket", pathStyleBucket);
    }

    public Boolean getAsyncAccess() {
        return getAsBoolean("AsyncAccess");
    }

    public JADEOptions setAsyncAccess(Boolean asyncAccess) {
        return set("AsyncAccess", asyncAccess);
    }

    public JADEOptions setDefaultAsyncAccess(Boolean asyncAccess) {
        return setIfMissing("AsyncAccess", asyncAccess);
    }

    public Boolean getTryAnonymousAccessFirst() {
        return getAsBoolean("TryAnonymousAccessFirst");
    }

    public JADEOptions setTryAnonymousAccessFirst(Boolean tryAnonymousAccessFirst) {
        return set("TryAnonymousAccessFirst", tryAnonymousAccessFirst);
    }

    public JADEOptions setDefaultTryAnonymousAccessFirst(Boolean tryAnonymousAccessFirst) {
        return setIfMissing("TryAnonymousAccessFirst", tryAnonymousAccessFirst);
    }

}
