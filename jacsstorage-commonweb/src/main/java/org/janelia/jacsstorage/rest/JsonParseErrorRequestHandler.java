package org.janelia.jacsstorage.rest;

import jakarta.enterprise.context.RequestScoped;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@RequestScoped
public class JsonParseErrorRequestHandler implements ExceptionMapper<JsonParseException> {
    private static final Logger LOG = LoggerFactory.getLogger(IllegalStateRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(JsonParseException exception) {
        LOG.error("Error parsing JSON for {}", request.getMethod(), exception);
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.BAD_REQUEST);
        if (!StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Invalid JSON request body");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
