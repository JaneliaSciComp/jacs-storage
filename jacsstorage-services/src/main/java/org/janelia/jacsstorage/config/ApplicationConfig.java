package org.janelia.jacsstorage.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public interface ApplicationConfig {
    String getStringPropertyValue(String name);
    String getStringPropertyValue(String name, String defaultValue);
    Boolean getBooleanPropertyValue(String name);
    Integer getIntegerPropertyValue(String name);
    Integer getIntegerPropertyValue(String name, Integer defaultValue);
    Long getLongPropertyValue(String name);
    Long getLongPropertyValue(String name, Long defaultValue);
    void load(InputStream stream) throws IOException;
    void putAll(Map<String, String> properties);
}
