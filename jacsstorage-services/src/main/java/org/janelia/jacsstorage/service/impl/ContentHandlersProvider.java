package org.janelia.jacsstorage.service.impl;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import com.google.common.collect.Streams;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.service.impl.contenthandling.NoFilter;
import org.janelia.jacsstorage.service.impl.contenthandling.SimpleMetadataReader;

public class ContentHandlersProvider {

    private final Instance<ContentFilter> contentFilterProvider;
    private final Instance<ContentMetadataReader> contentMetadataReaderProvider;


    @Inject
    public ContentHandlersProvider(Instance<ContentFilter> contentFilterProvider,
                                   Instance<ContentMetadataReader> contentMetadataReaderProvider) {
        this.contentFilterProvider = contentFilterProvider;
        this.contentMetadataReaderProvider = contentMetadataReaderProvider;
    }

    ContentFilter getContentFilter(ContentFilterParams contentFilterParams) {
        return Streams.stream(contentFilterProvider)
                .filter(contentStreamFilter -> contentFilterParams != null && contentStreamFilter.support(contentFilterParams.getFilterType()))
                .findFirst()
                .orElseGet(() -> new NoFilter(contentFilterParams != null && contentFilterParams.isAlwaysArchive()));
    }

    ContentMetadataReader getContentMetadataReader(String mimeType) {
        return Streams.stream(contentMetadataReaderProvider)
                .filter(contentMetadataReader -> contentMetadataReader.support(mimeType))
                .findFirst()
                .orElse(new SimpleMetadataReader());
    }

}
