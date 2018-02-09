package org.janelia.jacsstorage.rest;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Error response type")
@JacksonXmlRootElement(localName = "errorresponse")
public class ErrorResponse {
    @JacksonXmlProperty(localName = "errormessage")
    private final String errorMessage;

    public ErrorResponse(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @ApiModelProperty(value = "Error message")
    public String getErrorMessage() {
        return errorMessage;
    }
}
