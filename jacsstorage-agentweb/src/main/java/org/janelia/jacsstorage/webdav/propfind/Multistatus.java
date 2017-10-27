package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

@JacksonXmlRootElement(namespace = "D", localName = "multistatus")
public class Multistatus {
    @JacksonXmlProperty(namespace = "xmlns", localName = "D", isAttribute = true)
    private final String davNamespace = "DAV:";

    @JacksonXmlProperty(namespace = "xmlns", localName = "lp1", isAttribute = true)
    private final String apacheNamespace = "http://apache.org/dav/props/";

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(namespace = "D", localName = "response")
    private List<PropfindResponse> response = new ArrayList<>();

    public List<PropfindResponse> getResponse() {
        return response;
    }

    public void setResponse(List<PropfindResponse> response) {
        this.response = response;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("response", response)
                .build();
    }
}
