package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.service.impl.ContentAccess;
import org.janelia.jacsstorage.service.impl.ContentMetadataReader;

public interface ContentAccessProvider {
    ContentAccess getContentFilter(ContentAccessParams contentAccessParams);
    ContentMetadataReader getContentMetadataReader(String mimeType);
}
