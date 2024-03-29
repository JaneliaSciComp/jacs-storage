package org.janelia.jacsstorage.config;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ApplicationConfigImpl implements ApplicationConfig {
    private final Map<String, String> configProperties = new HashMap<>();

    private final ApplicationConfigValueResolver configValueResolver = new ApplicationConfigValueResolver();

    @Override
    public String getStringPropertyValue(String name) {
        String value = configProperties.get(name);
        return configValueResolver.resolve(value, configProperties::get);
    }

    @Override
    public String getStringPropertyValue(String name, String defaultValue) {
        String value = getStringPropertyValue(name);
        return StringUtils.isBlank(value) ? defaultValue : value;
    }

    @Override
    public Boolean getBooleanPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? Boolean.FALSE : Boolean.valueOf(stringValue);
    }

    @Override
    public Boolean getBooleanPropertyValue(String name, Boolean defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? defaultValue : Boolean.valueOf(stringValue);
    }

    @Override
    public Double getDoublePropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? null : Double.valueOf(stringValue);
    }

    @Override
    public Double getDoublePropertyValue(String name, Double defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? defaultValue : Double.valueOf(stringValue);
    }

    @Override
    public Integer getIntegerPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? null : Integer.valueOf(stringValue);
    }

    @Override
    public Integer getIntegerPropertyValue(String name, Integer defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? defaultValue : Integer.valueOf(stringValue);
    }

    @Override
    public Long getLongPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? null : Long.valueOf(stringValue);
    }

    @Override
    public Long getLongPropertyValue(String name, Long defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? defaultValue : Long.valueOf(stringValue);
    }

    @Override
    public List<String> getStringListPropertyValue(String name) {
        return getStringListPropertyValue(name, ImmutableList.of());
    }

    @Override
    public List<String> getStringListPropertyValue(String name, List<String> defaultValue) {
        String stringValue = getStringPropertyValue(name);
        if (StringUtils.isBlank(stringValue)) {
            return defaultValue == null ? ImmutableList.of() : ImmutableList.copyOf(defaultValue);
        } else {
            return Splitter.on(',').trimResults().splitToList(stringValue);
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return configProperties.containsKey(name);
    }

    @Override
    public void load(InputStream stream) throws IOException {
        Properties toLoad = new Properties();
        toLoad.load(stream);
        putAll(Maps.fromProperties(toLoad));
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
        configProperties.keySet().forEach(k -> {
            String v = getStringPropertyValue(k);
            if (k != null && v != null) builder.put(k, v);
        });
        return builder.build();
    }

}
