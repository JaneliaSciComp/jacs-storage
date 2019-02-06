package org.janelia.jacsstorage.io;

import com.google.common.collect.Streams;
import org.janelia.jacsstorage.io.contenthandlers.IDContentStreamFilter;
import org.janelia.jacsstorage.io.contenthandlers.VoidContentInfoExtractor;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class ContentHandlerProvider {

    private final Instance<ContentStreamFilter> filterSource;
    private final Instance<ContentInfoExtractor> contentInfoExtractorsSource;

    @Inject
    public ContentHandlerProvider(Instance<ContentStreamFilter> filterSource,
                                  Instance<ContentInfoExtractor> contentInfoExtractorsSource) {
        this.filterSource = filterSource;
        this.contentInfoExtractorsSource = contentInfoExtractorsSource;
    }

    public ContentStreamFilter getContentStreamFilter(ContentFilterParams contentFilterParams) {
        return Streams.stream(filterSource)
                .filter(contentStreamFilter -> contentFilterParams != null && contentStreamFilter.support(contentFilterParams.getFilterType()))
                .findFirst()
                .orElseGet(() -> new IDContentStreamFilter());
    }

    public ContentInfoExtractor getContentInfoExtractor(String mimeType) {
        return Streams.stream(contentInfoExtractorsSource)
                .filter(contentInfoExtractor -> contentInfoExtractor.support(mimeType))
                .findFirst()
                .orElseGet(() -> new VoidContentInfoExtractor());
    }

}
