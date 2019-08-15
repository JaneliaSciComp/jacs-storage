package org.janelia.jacsstorage.io;

import org.apache.commons.lang3.builder.ToStringBuilder;

abstract class AbstractDataContent implements DataContent {
    private final ContentFilterParams contentFilterParams;

    AbstractDataContent(ContentFilterParams contentFilterParams) {
        this.contentFilterParams = contentFilterParams;
    }

    public ContentFilterParams getContentFilterParams() {
        return contentFilterParams;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("contentFilterParams", contentFilterParams)
                .toString();
    }
}
