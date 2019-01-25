package org.janelia.jacsstorage.io;

public interface ContentStreamFilter {
    boolean support(String filterType);
    ContentInputStream apply(ContentFilterParams filterParams, ContentInputStream stream);
}
