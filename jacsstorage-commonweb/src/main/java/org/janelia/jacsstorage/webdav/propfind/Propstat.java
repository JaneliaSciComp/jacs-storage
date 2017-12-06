package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Propstat {
    @JacksonXmlProperty(localName = "D:prop")
    private Prop prop;

    @JacksonXmlProperty(localName = "D:status")
    private String status;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Prop getProp() {
        return prop;
    }

    public void setProp(Prop prop) {
        this.prop = prop;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("prop", prop)
                .append("status", status)
                .build();
    }
}

