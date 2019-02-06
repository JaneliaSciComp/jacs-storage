package org.janelia.jacsstorage.io.contenthandlers;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.io.ContentInfoExtractor;

import javax.enterprise.inject.Vetoed;
import java.io.InputStream;
import java.util.Map;

@Vetoed
public class VoidContentInfoExtractor implements ContentInfoExtractor {
    @Override
    public boolean support(String mimeType) {
        return false;
    }

    @Override
    public Map<String, Object> extractContentInfo(InputStream inputStream) {
        return ImmutableMap.of();
    }
}
