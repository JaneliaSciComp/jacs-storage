package org.janelia.jacsstorage.io.contenthandlers;

import java.io.InputStream;
import java.util.Map;

import javax.enterprise.inject.Vetoed;

import com.google.common.collect.ImmutableMap;

import org.janelia.jacsstorage.io.ContentInfoExtractor;

@Vetoed
public class EmptyContentInfoExtractor implements ContentInfoExtractor {
    @Override
    public boolean support(String mimeType) {
        return false;
    }

    @Override
    public Map<String, Object> extractContentInfo(InputStream inputStream) {
        return ImmutableMap.of();
    }
}
