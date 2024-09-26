package org.janelia.jacsstorage.service.impl.contenthandling;

import java.util.Collections;
import java.util.Map;

import javax.enterprise.inject.Vetoed;

import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.impl.ContentMetadataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Vetoed
public class NoOpMetadataReader implements ContentMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(NoOpMetadataReader.class);

    @Override
    public boolean support(String mimeType) {
        return false;
    }

    @Override
    public Map<String, Object> getMetadata(ContentNode contentNode) {
        return Collections.emptyMap();
    }
}
