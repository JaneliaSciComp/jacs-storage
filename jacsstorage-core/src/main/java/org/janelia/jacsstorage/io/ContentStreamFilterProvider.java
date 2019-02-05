package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.io.contentfilters.IDContentStreamFilter;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class ContentStreamFilterProvider {

    private final Instance<ContentStreamFilter> filterSource;

    @Inject
    public ContentStreamFilterProvider(Instance<ContentStreamFilter> filterSource) {
        this.filterSource = filterSource;
    }

    public ContentStreamFilter getContentStreamFilter(ContentFilterParams contentFilterParams) {
        if (contentFilterParams != null) {
            for (ContentStreamFilter streamFilter : getSupportedFilters(filterSource)) {
                if (streamFilter.support(contentFilterParams.getFilterType())) {
                    return streamFilter;
                }
            }
        }
        return new IDContentStreamFilter();
    }

    private List<ContentStreamFilter> getSupportedFilters(Instance<ContentStreamFilter> streamFilters) {
        List<ContentStreamFilter> supportedFilters = new ArrayList<>();
        for (ContentStreamFilter streamFilter : streamFilters) {
            supportedFilters.add(streamFilter);
        }
        return supportedFilters;
    }

}
