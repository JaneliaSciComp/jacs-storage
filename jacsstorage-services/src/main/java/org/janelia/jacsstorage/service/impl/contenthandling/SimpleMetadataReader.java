package org.janelia.jacsstorage.service.impl.contenthandling;

import java.util.Map;

import jakarta.enterprise.inject.Vetoed;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStreamReader;
import org.janelia.jacsstorage.service.impl.ContentMetadataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Vetoed
public class SimpleMetadataReader implements ContentMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleMetadataReader.class);

    @Override
    public boolean support(String mimeType) {
        return false;
    }

    @Override
    public Map<String, Object> getMetadata(ContentNode contentNode, ContentStreamReader contentObjectReader) {
        return ImmutableMap.<String, Object>builder()
                .put("size", contentNode.getSize())
                .build();
    }
}
