package org.janelia.jacsstorage.io;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import com.google.common.collect.Streams;

import org.janelia.jacsstorage.io.contenthandlers.EmptyContentInfoExtractor;
import org.janelia.jacsstorage.io.contenthandlers.NoOPContentConverter;

public class OriginalContentHandlerProvider {

    private final Instance<ContentConverter> contentConverterSource;
    private final Instance<ContentInfoExtractor> contentInfoExtractorsSource;

    @Inject
    public OriginalContentHandlerProvider(Instance<ContentConverter> contentConverterSource,
                                          Instance<ContentInfoExtractor> contentInfoExtractorsSource) {
        this.contentConverterSource = contentConverterSource;
        this.contentInfoExtractorsSource = contentInfoExtractorsSource;
    }

    public ContentConverter getContentConverter(ContentFilterParams contentFilterParams) {
        return Streams.stream(contentConverterSource)
                .filter(contentStreamFilter -> contentFilterParams != null && contentStreamFilter.support(contentFilterParams.getFilterType()))
                .findFirst()
                .orElseGet(() -> new NoOPContentConverter(contentFilterParams.isAlwaysArchive()));
    }

    public ContentInfoExtractor getContentInfoExtractor(String mimeType) {
        return Streams.stream(contentInfoExtractorsSource)
                .filter(contentInfoExtractor -> contentInfoExtractor.support(mimeType))
                .findFirst()
                .orElseGet(() -> new EmptyContentInfoExtractor());
    }

}
