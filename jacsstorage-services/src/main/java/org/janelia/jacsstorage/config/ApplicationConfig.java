package org.janelia.jacsstorage.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface ApplicationConfig {
    String getStringPropertyValue(String name);
    String getStringPropertyValue(String name, String defaultValue);
    Boolean getBooleanPropertyValue(String name);
    Boolean getBooleanPropertyValue(String name, Boolean defaultValue);
    Double getDoublePropertyValue(String name);
    Double getDoublePropertyValue(String name, Double defaultValue);
    Integer getIntegerPropertyValue(String name);
    Integer getIntegerPropertyValue(String name, Integer defaultValue);
    Long getLongPropertyValue(String name);
    Long getLongPropertyValue(String name, Long defaultValue);
    List<String> getStringListPropertyValue(String name);
    List<String> getStringListPropertyValue(String name, List<String> defaultValue);
    boolean hasProperty(String name);
    void load(InputStream stream) throws IOException;
    void put(String key, String value);
    void putAll(Map<String, String> properties);
    Map<String, String> asMap();
}
