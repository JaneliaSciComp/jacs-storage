package org.janelia.jacsstorage.io;

abstract class AbstractDataContent implements DataContent {
    private final ContentFilterParams contentFilterParams;

    AbstractDataContent(ContentFilterParams contentFilterParams) {
        this.contentFilterParams = contentFilterParams;
    }

    public ContentFilterParams getContentFilterParams() {
        return contentFilterParams;
    }
}
