package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Propstat {
    @JacksonXmlProperty(localName = "D:prop")
    private PropContainer propContainer;

    @JacksonXmlProperty(localName = "D:status")
    private String status;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public PropContainer getPropContainer() {
        return propContainer;
    }

    public void setPropContainer(PropContainer propContainer) {
        this.propContainer = propContainer;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("prop", propContainer)
                .append("status", status)
                .build();
    }
}

