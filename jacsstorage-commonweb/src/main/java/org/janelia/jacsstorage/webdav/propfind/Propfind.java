package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JacksonXmlRootElement(namespace = "D", localName = "propfind")
public class Propfind {
    @JacksonXmlProperty(namespace = "xmlns:D", isAttribute = true)
    private final String davNamespace = "DAV:";

    @JacksonXmlProperty(namespace = "D", localName = "prop")
    private QueryProp prop;

    public QueryProp getProp() {
        return prop;
    }

    public void setProp(QueryProp prop) {
        this.prop = prop;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("prop", prop)
                .build();
    }
}
