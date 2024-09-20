package org.janelia.jacsstorage.service.impl;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import com.google.common.collect.Streams;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.service.impl.content_filters.NoFilter;

public class ContentFilterProvider {

    private final Instance<ContentFilter> contentFilterSource;

    @Inject
    public ContentFilterProvider(Instance<ContentFilter> contentFilterSource) {
        this.contentFilterSource = contentFilterSource;
    }

    ContentFilter getContentFilter(ContentFilterParams contentFilterParams) {
        return Streams.stream(contentFilterSource)
                .filter(contentStreamFilter -> contentFilterParams != null && contentStreamFilter.support(contentFilterParams.getFilterType()))
                .findFirst()
                .orElseGet(() -> new NoFilter(contentFilterParams != null && contentFilterParams.isAlwaysArchive()));
    }

}
