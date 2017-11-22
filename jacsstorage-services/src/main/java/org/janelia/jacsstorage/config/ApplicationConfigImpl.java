package org.janelia.jacsstorage.config;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
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
    public List<String> getStringListPropertyValue(String name) {
        return getStringListPropertyValue(name, ImmutableList.of());
    }

    @Override
    public List<String> getStringListPropertyValue(String name, List<String> defaultValue) {
        String stringValue = getStringPropertyValue(name);
        if (stringValue == null) {
            return ImmutableList.of();
        } else {
            return Splitter.on(',').trimResults().splitToList(stringValue);
        }

    }

    @Override
    public void load(InputStream stream) throws IOException {
        configProperties.load(stream);
    }

    @Override
    public void put(String key, String value) {
        configProperties.put(key, value);
    }

    @Override
    public void putAll(Map<String, String> properties) {
        configProperties.putAll(properties);
    }

    @Override
    public Map<String, String> asMap() {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        configProperties.stringPropertyNames().forEach(k -> {
            String v = configProperties.getProperty(k);
            if (k != null && v != null) builder.put(k, v);
        });
        return builder.build();
    }

}
