package org.janelia.jacsstorage.io.contentfilters;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.io.ContentInputStream;
import org.janelia.jacsstorage.io.ContentStreamFilter;

import javax.enterprise.inject.Vetoed;

@Vetoed
public class IDContentStreamFilter implements ContentStreamFilter {

    @Override
    public boolean support(String filterType) {
        return true;
    }

    public ContentInputStream apply(ContentFilterParams filterParams, ContentInputStream stream) {
        return stream;
    }
}
