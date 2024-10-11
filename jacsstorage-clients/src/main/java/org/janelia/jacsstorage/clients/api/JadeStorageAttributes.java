package org.janelia.jacsstorage.clients.api;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JadeStorageAttributes {
    private final Map<String, Object> attributes = new HashMap<>();

    public Collection<String> getAttributeNames() {
        return attributes.keySet();
    }

    public Object getAttributeValue(String attributeName) {
        return attributes.get(attributeName);
    }

    public JadeStorageAttributes setAttributeValue(String attributeName, String attributeValue) {
        if (attributeValue != null) {
            attributes.put(attributeName, attributeValue);
        }
        return this;
    }

    public Map<String, Object> getAsMap() {
        return Collections.unmodifiableMap(attributes);
    }

    public JadeStorageAttributes setFromMap(Map<String, Object> map) {
        attributes.putAll(map);
        return this;
    }
}
