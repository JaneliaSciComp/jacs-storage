package org.janelia.jacsstorage.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class ApplicationConfigImpl implements ApplicationConfig {
    private final Properties configProperties = new Properties();

    @Override
    public String getStringPropertyValue(String name) {
        return configProperties.getProperty(name);
    }

    @Override
    public String getStringPropertyValue(String name, String defaultValue) {
        return configProperties.getProperty(name, defaultValue);
    }

    @Override
    public Boolean getBooleanPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? false : Boolean.valueOf(stringValue);
    }

    @Override
    public Integer getIntegerPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? null : Integer.valueOf(stringValue);
    }

    @Override
    public Integer getIntegerPropertyValue(String name, Integer defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? defaultValue : Integer.valueOf(stringValue);
    }

    @Override
    public Long getLongPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? null : Long.valueOf(stringValue);
    }

    @Override
    public Long getLongPropertyValue(String name, Long defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? defaultValue : Long.valueOf(stringValue);
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
