package org.janelia.jacsstorage.io;

import org.apache.commons.lang3.builder.ToStringBuilder;

abstract class AbstractDataContent implements DataContent {
    private final ContentAccessParams contentAccessParams;

    AbstractDataContent(ContentAccessParams contentAccessParams) {
        this.contentAccessParams = contentAccessParams;
    }

    public ContentAccessParams getContentFilterParams() {
        return contentAccessParams;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("contentFilterParams", contentAccessParams)
                .toString();
    }
}
