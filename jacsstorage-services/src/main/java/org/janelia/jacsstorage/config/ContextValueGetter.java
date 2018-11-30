package org.janelia.jacsstorage.config;

@FunctionalInterface
public interface ContextValueGetter {
    String get(String key);
}
