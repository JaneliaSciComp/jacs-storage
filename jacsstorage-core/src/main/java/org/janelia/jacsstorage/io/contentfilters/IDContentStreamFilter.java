package org.janelia.jacsstorage.io.contentfilters;

import org.janelia.jacsstorage.io.ContentFilteredInputStream;
import org.janelia.jacsstorage.io.ContentStreamFilter;

import javax.enterprise.inject.Vetoed;

@Vetoed
public class IDContentStreamFilter implements ContentStreamFilter {

    @Override
    public boolean support(String filterType) {
        return true;
    }

    public ContentFilteredInputStream apply(ContentFilteredInputStream stream) {
        return stream;
    }
}
