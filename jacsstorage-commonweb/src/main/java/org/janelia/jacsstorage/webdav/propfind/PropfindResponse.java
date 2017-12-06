package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class PropfindResponse {
    @JacksonXmlProperty(localName = "D:href")
    private String href;

    @JacksonXmlProperty(localName = "D:propstat")
    private Propstat propstat;

    public Propstat getPropstat() {
        return propstat;
    }

    public void setPropstat(Propstat propstat) {
        this.propstat = propstat;
    }

    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("propstat", propstat)
                .build();
    }
}

