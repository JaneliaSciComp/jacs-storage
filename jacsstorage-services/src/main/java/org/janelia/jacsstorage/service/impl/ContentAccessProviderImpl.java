package org.janelia.jacsstorage.service.impl;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.google.common.collect.Streams;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentAccessProvider;
import org.janelia.jacsstorage.service.impl.contenthandling.DirectContentAccess;
import org.janelia.jacsstorage.service.impl.contenthandling.SimpleMetadataReader;

@Dependent
public class ContentAccessProviderImpl implements ContentAccessProvider {

    private final Instance<ContentAccess> contentAccessProvider;
    private final Instance<ContentMetadataReader> contentMetadataReaderProvider;

    @Inject
    public ContentAccessProviderImpl(Instance<ContentAccess> contentAccessProvider,
                                     Instance<ContentMetadataReader> contentMetadataReaderProvider) {
        this.contentAccessProvider = contentAccessProvider;
        this.contentMetadataReaderProvider = contentMetadataReaderProvider;
    }

    public ContentAccess getContentFilter(ContentAccessParams contentAccessParams) {
        return Streams.stream(contentAccessProvider)
                .filter(contentStreamFilter -> contentAccessParams != null && contentStreamFilter.isAccessTypeSupported(contentAccessParams.getFilterType()))
                .findFirst()
                .orElseGet(() -> new DirectContentAccess(contentAccessParams != null && contentAccessParams.isAlwaysArchive()));
    }

    public ContentMetadataReader getContentMetadataReader(String mimeType) {
        return Streams.stream(contentMetadataReaderProvider)
                .filter(contentMetadataReader -> contentMetadataReader.support(mimeType))
                .findFirst()
                .orElse(new SimpleMetadataReader());
    }

}
