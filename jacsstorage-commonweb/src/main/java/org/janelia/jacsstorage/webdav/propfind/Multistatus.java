package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

@JacksonXmlRootElement(localName = "D:multistatus")
public class Multistatus {
    @JacksonXmlProperty(localName = "xmlns:D", isAttribute = true)
    final String davNamespace = "DAV:";

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "D:response")
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
