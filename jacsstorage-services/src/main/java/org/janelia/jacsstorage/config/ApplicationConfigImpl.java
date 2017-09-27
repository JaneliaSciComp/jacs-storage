package org.janelia.jacsstorage.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class ApplicationConfigImpl implements ApplicationConfig {
    private final Properties configProperties = new Properties();

    public String getStringPropertyValue(String name) {
        return configProperties.getProperty(name);
    }

    public String getStringPropertyValue(String name, String defaultValue) {
        return configProperties.getProperty(name, defaultValue);
    }

    public Boolean getBooleanPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? false : Boolean.valueOf(stringValue);
    }

    public Integer getIntegerPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? null : Integer.valueOf(stringValue);
    }

    public Integer getIntegerPropertyValue(String name, Integer defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? defaultValue : Integer.valueOf(stringValue);
    }

    @Override
    public void load(InputStream stream) throws IOException {
        configProperties.load(stream);
    }

    @Override
    public void putAll(Map<String, String> properties) {
        configProperties.putAll(properties);
    }
}
