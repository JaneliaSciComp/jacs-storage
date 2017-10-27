package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JacksonXmlRootElement(namespace = "D", localName = "propfind")
public class Propfind {
    @JacksonXmlProperty(namespace = "xmlns", localName = "D", isAttribute = true)
    private final String davNamespace = "DAV:";

    @JacksonXmlProperty(namespace = "D", localName = "prop")
    private Prop prop;

    public String getDavNamespace() {
        return davNamespace;
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
                .build();
    }
}
