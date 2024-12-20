package org.janelia.jacsstorage.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.builder.ToStringBuilder;

@Schema(description = "Error response type")
@JacksonXmlRootElement(localName = "errorresponse")
public class ErrorResponse {
    @JacksonXmlProperty(localName = "errormessage")
    private final String errorMessage;

    @JsonCreator
    public ErrorResponse(@JsonProperty("errormessage") String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Schema(description = "Error message")
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("errorMessage", errorMessage)
                .toString();
    }
}
