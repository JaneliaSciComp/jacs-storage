package org.janelia.jacsstorage.io;

public interface ContentStreamFilter {
    boolean support(String filterType);
    ContentFilteredInputStream apply(ContentFilteredInputStream stream);
}
